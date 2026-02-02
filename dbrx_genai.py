#1. Create a secret scope (one-time setup)

databricks secrets create-scope --scope genai-secrets


Add your keys:

databricks secrets put --scope genai-secrets --key AZURE_OPENAI_KEY
databricks secrets put --scope genai-secrets --key AZURE_OPENAI_ENDPOINT


#2. Access secrets safely in Python (Databricks)
azure_openai_key = dbutils.secrets.get(
    scope="genai-secrets",
    key="AZURE_OPENAI_KEY"
)

azure_openai_endpoint = dbutils.secrets.get(
    scope="genai-secrets",
    key="AZURE_OPENAI_ENDPOINT"
)

#🔹 3. Use secrets while building the GenAI model
# Example: Azure OpenAI + LangChain
from langchain.chat_models import AzureChatOpenAI

llm = AzureChatOpenAI(
    api_key=azure_openai_key,
    azure_endpoint=azure_openai_endpoint,
    deployment_name="gpt-4",
    api_version="2024-02-15-preview",
    temperature=0.2
)

#4. Log the model to MLflow (secrets NOT stored)
import mlflow
import mlflow.langchain

with mlflow.start_run():
    mlflow.langchain.log_model(
        chain=llm,
        artifact_path="genai_model",
        input_example={"question": "What is Unity Catalog?"},
    )

#5. Grant secret access securely

Secrets are workspace-scoped, so control access carefully:

databricks secrets put-acl \
  --scope genai-secrets \
  --principal data_scientists \
  --permission READ

#6. Load UC model & invoke (secrets auto-resolved)
import mlflow.langchain

model = mlflow.langchain.load_model(
    "models:/main.ai_models.genai_chatbot/Production"
)

model.invoke({"question": "Explain Unity Catalog governance"})

#7. Serving Endpoint + Secrets (BEST PRACTICE)

import requests, json

headers = {
  "Authorization": f"Bearer {dbutils.secrets.get('genai-secrets','DATABRICKS_PAT')}",
  "Content-Type": "application/json"
}

requests.post(
  "https://<workspace>/serving-endpoints/genai_chatbot/invocations",
  headers=headers,
  data=json.dumps({"inputs": {"question": "Hello"}})
)


