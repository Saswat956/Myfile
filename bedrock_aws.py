import json
import os
import sys
import boto3
import numpy as np
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain_community.document_loaders import PyPDFDirectoryLoader
from langchain.prompts import PromptTemplate
from langchain.chains import RetrievalQA

## OpenSearch Client
from opensearchpy import OpenSearch

## LangChain OpenSearch Integration
from langchain.vectorstores import OpenSearchVectorSearch

## AWS Bedrock Clients
from langchain_community.embeddings import BedrockEmbeddings
from langchain.llms.bedrock import Bedrock

# Initialize OpenSearch
OPENSEARCH_HOST = "https://your-opensearch-domain"  # Change to your OpenSearch endpoint
INDEX_NAME = "pdf_embeddings"  # Change if needed

opensearch_client = OpenSearch(
    hosts=[OPENSEARCH_HOST],
    http_auth=("admin", "admin"),  # Change credentials
    use_ssl=True,
    verify_certs=False,
)

# Initialize AWS Bedrock
bedrock = boto3.client(service_name="bedrock-runtime")
bedrock_embeddings = BedrockEmbeddings(model_id="amazon.titan-embed-text-v1", client=bedrock)


## Data ingestion
def data_ingestion():
    loader = PyPDFDirectoryLoader("data")
    documents = loader.load()

    # Character split works better for PDF data
    text_splitter = RecursiveCharacterTextSplitter(chunk_size=10000, chunk_overlap=1000)
    docs = text_splitter.split_documents(documents)
    return docs


## Store embeddings in OpenSearch
def store_embeddings_in_opensearch(docs):
    vector_store = OpenSearchVectorSearch.from_documents(
        documents=docs,
        embedding=bedrock_embeddings,
        opensearch_url=OPENSEARCH_HOST,
        index_name=INDEX_NAME,
    )
    print("Embeddings stored successfully in OpenSearch.")


def get_claude_llm():
    """Create the Claude model from Bedrock"""
    llm = Bedrock(model_id="ai21.j2-mid-v1", client=bedrock, model_kwargs={'maxTokens': 512})
    return llm


def get_llama2_llm():
    """Create the Llama2 model from Bedrock"""
    llm = Bedrock(model_id="meta.llama2-70b-chat-v1", client=bedrock, model_kwargs={'max_gen_len': 512})
    return llm


prompt_template = """
Human: Use the following pieces of context to provide a 
concise answer to the question at the end but summarize with 
at least 250 words with detailed explanations. If you don't know the answer, 
just say that you don't know, don't try to make up an answer.

<context>
{context}
</context>

Question: {question}

Assistant:"""

PROMPT = PromptTemplate(template=prompt_template, input_variables=["context", "question"])


def get_response_llm(llm, query):
    """Retrieve embeddings from OpenSearch and generate a response."""
    
    # Initialize OpenSearch vector store retriever
    vector_store = OpenSearchVectorSearch(
        embedding_function=bedrock_embeddings,
        opensearch_url=OPENSEARCH_HOST,
        index_name=INDEX_NAME,
    )
    
    retriever = vector_store.as_retriever(search_type="similarity", search_kwargs={"k": 3})

    qa = RetrievalQA.from_chain_type(
        llm=llm,
        chain_type="stuff",
        retriever=retriever,
        return_source_documents=True,
        chain_type_kwargs={"prompt": PROMPT},
    )

    answer = qa({"query": query})
    return answer['result']


def main():
    while True:
        print("\nOptions:")
        print("1. Update/Create Vector Store in OpenSearch")
        print("2. Get Claude Output")
        print("3. Get Llama2 Output")
        print("4. Exit")

        choice = input("Enter your choice: ")

        if choice == "1":
            print("Processing data ingestion and OpenSearch vector store update...")
            docs = data_ingestion()
            store_embeddings_in_opensearch(docs)
            print("Vector store updated successfully in OpenSearch!")

        elif choice == "2":
            user_question = input("\nEnter your question: ")
            llm = get_claude_llm()
            print("\nClaude Output:\n")
            print(get_response_llm(llm, user_question))

        elif choice == "3":
            user_question = input("\nEnter your question: ")
            llm = get_llama2_llm()
            print("\nLlama2 Output:\n")
            print(get_response_llm(llm, user_question))

        elif choice == "4":
            print("Exiting...")
            break

        else:
            print("Invalid choice! Please try again.")


if __name__ == "__main__":
    main()