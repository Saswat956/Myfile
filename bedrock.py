import boto3

def classify_topic_with_bedrock(words):
    # Format words as a comma-separated string
    words_str = ", ".join(words)
    
    # Create the prompt
    prompt = f"""
    Given the following words:
    [{words_str}]
    
    Determine if these words belong to the same topic. If they do, suggest a name for the topic. If they belong to multiple topics, group the words by topic and name each group.
    """
    
    # Connect to AWS Bedrock
    client = boto3.client('bedrock-runtime')

    # Invoke the Bedrock LLM
    response = client.invoke_model(
        modelId='your-bedrock-model-id',  # Replace with your LLM model ID
        contentType='application/json',
        accept='application/json',
        body={"prompt": prompt}
    )
    
    # Extract and return the LLM response
    result = response['body'].read().decode('utf-8')
    return result

# Example usage
words = ['network', 'server', 'latency', 'outage', 'downtime', 'connectivity', 'bandwidth']
result = classify_topic_with_bedrock(words)
print("LLM Response:", result)
