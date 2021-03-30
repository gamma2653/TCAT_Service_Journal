from flask import Flask, request
from service_journal.command_line_tools import run_days
import service_journal as sj
app = Flask(__name__)


@app.route('/', methods=['GET', 'POST'])
def index():
    return {
        'status': 'Ready',
        'version': sj.__version__
    }


@app.route('/run', methods=['GET', 'POST'])
def run():
    if request.method == 'GET':
        return 'Ready to run.'
    elif request.method == 'POST':
        try:
            cmd = request.form['cmd']
            date_range = request.form['date_range']
        except IndexError:
            return 'Missing command parameter.', 400
        run_days(date_range)
        return 'Success'
    else:
        return 'Invalid method.', 405