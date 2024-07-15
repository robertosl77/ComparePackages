from flask import Flask, render_template
from views import ComparativaView

app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():
    return render_template('index.html')

@app.route('/comparativa', methods=['POST'])
def comparativa():
    return ComparativaView.comparativa()

if __name__ == '__main__':
    app.run(debug=True)
