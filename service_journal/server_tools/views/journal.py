"""
This is the journal admin control feature of the TCAT Service Journal.
"""
from flask.blueprints import Blueprint
from flask import request, jsonify, render_template

try:
    from ... import service_journal as sj
except ImportError:
    import service_journal as sj

journal_bp = Blueprint('journal', __name__, url_prefix='/journal')

@journal_bp.route('/', methods=['GET', 'POST'])
def journal_index():
    return render_template('journal.jinja')
