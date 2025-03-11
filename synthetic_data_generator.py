pip install pandas langchain groq openai


import pandas as pd

file_path = "input_data.xlsx"  # Change this to your actual file

df = pd.read_csv(file_path) if file_path.endswith('.csv') else pd.read_excel(file_path)

schema = {}
for col in df.columns:
    dtype = str(df[col].dtype)
    unique_values = df[col].nunique()
    
    if dtype == 'object':
        sample_values = df[col].dropna().unique()[:5].tolist()
        schema[col] = {"type": "categorical", "samples": sample_values}
    elif dtype in ['int64', 'float64']:
        schema[col] = {
            "type": "numeric",
            "min": df[col].min(),
            "max": df[col].max(),
            "mean": df[col].mean(),
            "std": df[col].std()
        }

print("Schema Analysis Completed:", schema)



prompt = "Generate synthetic data with the following schema:\n\n"

for col, details in schema.items():
    if details["type"] == "categorical":
        prompt += f"- {col}: One of {details['samples']}\n"
    elif details["type"] == "numeric":
        prompt += f"- {col}: A numeric value between {details['min']} and {details['max']} (mean: {details['mean']}, std: {details['std']})\n"

prompt += "\nGenerate 100 rows in CSV format."

print("Generated Prompt:\n", prompt)



from langchain.llms import Groq

llm = Groq(model="llama3-8b-8192", api_key="YOUR_GROQ_API_KEY")

response = llm.invoke(prompt)

print("LLM Response Received")

from io import StringIO

df_synthetic = pd.read_csv(StringIO(response))
df_synthetic.to_csv("synthetic_output.csv", index=False)

print("Synthetic data generated and saved successfully!")







