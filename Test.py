from flask import Flask, request, jsonify

app = Flask(__name__)

def split_into_paragraph_chunks(text, num_paragraphs):
    """Splits text into chunks, each containing a fixed number of paragraphs."""
    paragraphs = text.split("\n\n")  # Assuming paragraphs are separated by double newlines
    chunks = [paragraphs[i:i+num_paragraphs] for i in range(0, len(paragraphs), num_paragraphs)]
    formatted_chunks = ["\n\n".join(chunk) for chunk in chunks]  # Join paragraphs back into text chunks
    return formatted_chunks

@app.route('/process_markdown', methods=['POST'])
def process_markdown():
    """Flask route to receive and process Markdown text."""
    markdown_text = request.json.get("markdown_text")
    if not markdown_text:
        return jsonify({"error": "No markdown text provided"}), 400

    # Split into 8-paragraph chunks
    chunks = split_into_paragraph_chunks(markdown_text, 8)

    return jsonify({"total_chunks": len(chunks), "chunks": chunks})

if __name__ == '__main__':
    app.run(debug=True)
