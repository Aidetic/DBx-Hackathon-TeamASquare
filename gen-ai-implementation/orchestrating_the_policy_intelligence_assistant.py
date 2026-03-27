# Databricks notebook source
from pyspark.sql import functions as F

df = spark.table("primeinsurance.gold.dim_policy")

print("=== dim_policy schema ===")
print(df.columns)
print(f"\nRow count: {df.count()}")

print("\n=== Sample rows ===")
df.limit(3).show(truncate=False)

print("\n=== Check if sentence-transformers is available ===")
try:
    from sentence_transformers import SentenceTransformer
    print("sentence-transformers: INSTALLED")
except ImportError:
    print("sentence-transformers: NOT INSTALLED — need to pip install")

print("\n=== Check if faiss is available ===")
try:
    import faiss
    print("faiss: INSTALLED")
except ImportError:
    print("faiss: NOT INSTALLED — need to pip install")

# COMMAND ----------

# MAGIC %pip install sentence-transformers faiss-cpu --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from openai import OpenAI
from sentence_transformers import SentenceTransformer
import faiss
import numpy as np
import json, time
from datetime import datetime
from pyspark.sql import Row, functions as F

DATABRICKS_TOKEN = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
WORKSPACE_URL = spark.conf.get("spark.databricks.workspaceUrl")
client = OpenAI(api_key=DATABRICKS_TOKEN, base_url=f"https://{WORKSPACE_URL}/serving-endpoints")
MODEL_NAME = "databricks-gpt-oss-20b"

def extract_text(raw):
    if isinstance(raw, list):
        for block in raw:
            if isinstance(block, dict) and block.get("type") == "text":
                return block.get("text", str(raw))
        return str(raw)
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                for block in parsed:
                    if isinstance(block, dict) and block.get("type") == "text":
                        return block.get("text", raw)
        except:
            pass
        return raw
    return str(raw)
print("Step 1: Converting policy rows to natural language documents...")

df_policy = spark.table("primeinsurance.gold.dim_policy")
policies = df_policy.collect()

documents = []
policy_ids = []

for row in policies:
    r = row.asDict()
    umbrella_text = f"${r['umbrella_limit']:,} umbrella coverage" if r['umbrella_limit'] and r['umbrella_limit'] > 0 else "no umbrella coverage"
    
    doc = (
        f"Policy {r['policy_number']} is a {r['policy_csl_tier']}-tier auto insurance policy "
        f"in {r['policy_state']} with coverage limits of {r['policy_csl']} (CSL). "
        f"It was bound on {r['policy_bind_date']} with a ${r['policy_deductable']:,} deductible "
        f"and an annual premium of ${r['policy_annual_premium']:,.2f}. "
        f"This policy has {umbrella_text}. "
        f"It covers car {r['car_id']} for customer {r['customer_id']}."
    )
    
    documents.append(doc)
    policy_ids.append(str(r['policy_number']))

print(f"Created {len(documents)} policy documents")
print(f"Sample document:\n{documents[0]}\n")
print("Step 2: Generating embeddings with all-MiniLM-L6-v2...")

embed_model = SentenceTransformer('all-MiniLM-L6-v2')
embeddings = embed_model.encode(documents, show_progress_bar=True)
embeddings = np.array(embeddings).astype('float32')

print(f"Embedding shape: {embeddings.shape}") 
print("\nStep 3: Building FAISS index...")

dimension = embeddings.shape[1]  # 384
index = faiss.IndexFlatL2(dimension)
index.add(embeddings)

print(f"FAISS index built: {index.ntotal} vectors indexed")
def rag_query(question, top_k=5):
    """
    Retrieves top-k relevant policy documents and generates
    an LLM answer grounded in the retrieved context.
    Returns: answer, retrieved_policies, confidence_scores
    """
    q_embedding = embed_model.encode([question]).astype('float32')
    distances, indices = index.search(q_embedding, top_k)
    retrieved_docs = [documents[i] for i in indices[0]]
    retrieved_ids = [policy_ids[i] for i in indices[0]]
    confidence_scores = [float(1 / (1 + d)) for d in distances[0]]
    context = "\n\n".join([f"[Policy {pid}]: {doc}" for pid, doc in zip(retrieved_ids, retrieved_docs)])
    prompt = f"""You are a policy lookup assistant at PrimeInsurance. 
Answer the question using ONLY the policy information provided below.
Always cite specific policy numbers in your answer.
If the information is not in the provided policies, say so.

RETRIEVED POLICY DOCUMENTS:
{context}

QUESTION: {question}

Answer concisely, citing policy numbers for every claim you make."""
    
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=400,
            temperature=0.2
        )
        answer = extract_text(response.choices[0].message.content)
        status = "success"
    except Exception as e:
        answer = f"ERROR: {str(e)}"
        status = "failed"
    
    return answer, retrieved_ids, confidence_scores, status
print("\nStep 4: Running 5 test questions...\n")

test_questions = [
    "What are the coverage details for policy 100804?",
    "Which policies have the highest annual premiums?",
    "Which policies in Ohio have umbrella coverage?",
    "What policies have a $2000 deductible?",
    "How many High-tier policies exist and what states are they in?",
]

results = []
for i, question in enumerate(test_questions):
    print(f"Q{i+1}: {question}")
    
    answer, retrieved_ids, scores, status = rag_query(question, top_k=5)
    
    print(f"Retrieved policies: {retrieved_ids}")
    print(f"Confidence scores: {[f'{s:.3f}' for s in scores]}")
    print(f"Answer: {answer[:300]}...")
    print(f"Status: {status}")
    print("-" * 60)
    
    results.append(Row(
        query_id=f"RAG-Q{i+1}",
        question=question,
        answer=answer,
        retrieved_policy_ids=", ".join(retrieved_ids),
        confidence_scores=", ".join([f"{s:.4f}" for s in scores]),
        num_chunks_retrieved=len(retrieved_ids),
        model_name=MODEL_NAME,
        embedding_model="all-MiniLM-L6-v2",
        generation_status=status,
        query_type=["specific_lookup", "comparative", "filter_based", "deductible_inquiry", "coverage_tier"][i],
        generated_at=datetime.now()
    ))
    
    time.sleep(0.5)
df_results = spark.createDataFrame(results)
df_results.write.mode("overwrite").saveAsTable("primeinsurance.gold.rag_query_history")

print(f"\nWritten to primeinsurance.gold.rag_query_history")
print(f"Row count: {spark.table('primeinsurance.gold.rag_query_history').count()}")

# COMMAND ----------

df = spark.table("primeinsurance.gold.rag_query_history")
print(f"=== Total queries logged: {df.count()} ===\n")
rows = df.select("query_id", "question", "retrieved_policy_ids", 
                  "confidence_scores", "answer", "query_type").collect()
for row in rows:
    print(f"{'='*60}")
    print(f"{row.query_id} ({row.query_type})")
    print(f"Q: {row.question}")
    print(f"Retrieved: {row.retrieved_policy_ids}")
    print(f"Scores: {row.confidence_scores}")
    print(f"A: {row.answer[:400]}")
    print(f"{'='*60}\n")



# COMMAND ----------

spark.sql("ALTER MATERIALIZED VIEW primeinsurance.gold.dim_policy SET TBLPROPERTIES ('comment' = 'Policy dimension — 999 clean records with coverage type, premium, deductible, and umbrella details. One row per policy.')")
spark.sql("ALTER MATERIALIZED VIEW primeinsurance.gold.dim_customer SET TBLPROPERTIES ('comment' = 'Customer master dimension — 1604 deduplicated records unified from 7 regional source files. One row per unique customer.')")
spark.sql("ALTER MATERIALIZED VIEW primeinsurance.gold.fact_claims SET TBLPROPERTIES ('comment' = 'Claims fact table — 1000 records. Grain: one row per claim. Denormalized with policy_csl, region, and customer_id for performance.')")

print("Table-level descriptions added")

policy_comments = {
    "policy_number": "Unique identifier for each insurance policy",
    "policy_bind_date": "Date when the policy was bound/activated",
    "policy_state": "US state where policy is issued (IL, OH, NY, NC, SC, WV, IN, VA, PA)",
    "policy_csl": "Combined Single Limit. 100/300 = 100K per person/300K per accident. Values: 100/300, 250/500, 500/1000",
    "policy_csl_tier": "Coverage tier. Low = 100/300, Medium = 250/500, High = 500/1000",
    "policy_deductable": "Deductible in dollars. Values: 500, 1000, 2000",
    "policy_annual_premium": "Annual premium in dollars paid per year",
    "umbrella_limit": "Umbrella coverage in dollars. 0 = no umbrella. Positive values = additional liability",
    "car_id": "Foreign key to dim_car — insured vehicle",
    "customer_id": "Foreign key to dim_customer — policyholder",
}

customer_comments = {
    "customer_id": "Unique customer ID unified from 3 column name variants across regional files",
    "region": "Geographic region. Values: East, West, Central, South",
    "state": "US state of residence",
    "city": "City of residence",
    "job": "Job title or occupation",
    "marital": "Marital status: single, married, divorced. NULL if not captured",
    "education": "Education level: primary, secondary, tertiary, NA",
    "default_flag": "Default indicator. 1 = defaulted, 0 = no default",
    "balance": "Account balance in dollars",
    "hh_insurance": "Household insurance. 1 = has it, 0 = does not",
    "car_loan": "Car loan. 1 = active loan, 0 = no loan",
}

claims_comments = {
    "claim_id": "Unique claim identifier",
    "policy_id": "Foreign key to dim_policy",
    "customer_id": "Foreign key to dim_customer",
    "policy_csl": "Denormalized CSL: 100/300, 250/500, or 500/1000",
    "policy_csl_tier": "Denormalized tier: Low, Medium, High",
    "policy_deductable": "Denormalized deductible in dollars",
    "policy_annual_premium": "Denormalized annual premium in dollars",
    "region": "Denormalized region: East, West, Central, South",
    "state": "Denormalized US state of claimant",
    "incident_severity": "Minor Damage, Major Damage, or Trivial Damage",
    "incident_type": "Single Vehicle Collision, Multi-vehicle Collision, Vehicle Theft, Parked Car",
    "collision_type": "Rear, Side, Front, or unknown",
    "incident_state": "US state where incident occurred",
    "incident_city": "City where incident occurred",
    "authorities_contacted": "Police, Fire, Ambulance, Other, or None",
    "property_damage": "YES, NO, or NULL",
    "police_report_available": "YES, NO, or NULL",
    "claim_rejected": "Y = rejected, N = approved",
    "is_rejected": "1 = rejected, 0 = approved. Use SUM for counting",
    "injury_amount": "Claimed injury amount in dollars",
    "property_amount": "Claimed property damage in dollars",
    "vehicle_amount": "Claimed vehicle damage in dollars. NULL if not assessed",
    "total_claim_amount": "Total = injury + property + vehicle. Primary financial measure",
    "number_of_vehicles_involved": "Vehicles involved: 1, 2, 3, or 4",
    "bodily_injuries": "Bodily injuries: 0, 1, or 2",
    "witnesses": "Witnesses: 0, 1, 2, or 3",
}

tables = {
    "primeinsurance.gold.dim_policy": policy_comments,
    "primeinsurance.gold.dim_customer": customer_comments,
    "primeinsurance.gold.fact_claims": claims_comments,
}

for table, comments in tables.items():
    for col, comment in comments.items():
        try:
            spark.sql(f"COMMENT ON COLUMN {table}.{col} IS '{comment}'")
        except Exception as e:
            print(f"  {table}.{col}: {str(e)[:100]}")
    table_short = table.split(".")[-1]
    print(f" {table_short} column descriptions added")

print("\n=== All descriptions added successfully ===")

# COMMAND ----------

