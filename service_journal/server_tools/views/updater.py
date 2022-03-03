"""
This is the autoupdater feature of the TCAT Service Journal.
"""
from flask.blueprints import Blueprint

try:
    from ... import service_journal as sj
except ImportError:
    import service_journal as sj

updater_bp = Blueprint('updater', __name__, url_prefix='/updater')

@updater_bp.route('/', methods=['GET', 'POST'])
def index():
    return {
        'status': 'Ready',
        'version': sj.__version__
    }