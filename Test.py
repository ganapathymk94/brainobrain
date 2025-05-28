from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

def split_into_paragraph_chunks(text, num_paragraphs):
    """Splits text into chunks, each containing a fixed number of paragraphs."""
    paragraphs = text.split("\n\n")  # Assuming paragraphs are separated by double newlines
    chunks = [paragraphs[i:i+num_paragraphs] for i in range(0, len(paragraphs), num_paragraphs)]
    formatted_chunks = ["\n\n".join(chunk) for chunk in chunks]  # Join paragraphs back into text chunks
    return formatted_chunks

@app.route('/process_markdown', methods=['POST'])
def process_markdown():
    """Flask route to fetch and process a Markdown file from GitHub."""
    github_url = request.json.get("github_url")
    if not github_url:
        return jsonify({"error": "No GitHub URL provided"}), 400

    # Fetch file from GitHub
    response = requests.get(github_url)
    if response.status_code != 200:
        return jsonify({"error": "Failed to fetch file from GitHub"}), 400
    
    markdown_content = response.text

    # Split into 8-paragraph chunks
    chunks = split_into_paragraph_chunks(markdown_content, 8)

    return jsonify({"total_chunks": len(chunks), "chunks": chunks})

if __name__ == '__main__':
    app.run(debug=True)
