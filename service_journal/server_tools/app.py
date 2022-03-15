from flask import Flask, request, url_for, escape

from gamlogger import get_default_logger

try:
    from ... import service_journal as sj
    from .views.updater import updater_bp
    from .views.journal import journal_bp
except ImportError:
    from service_journal.server_tools.views.updater import updater_bp
    from service_journal.server_tools.views.journal import journal_bp
    import service_journal as sj

logger = get_default_logger(__name__)

def create_app():
    """
    Creates the flask app.

    This is the main entry point for the flask app. It is called by the web server.
    """
    app = Flask(__name__)

    @app.route('/', methods=['GET'])
    def index():
        if request.method == 'GET':
            logger.info('GET request received')
            return {
                'status': 'Ready',
                'version': sj.__version__
            }
        else:
            return 'Invalid request method.', 405
    
    @app.route('/static/<path:path>')
    def static_files(path):
        return url_for('static', filename=escape(path))

    app.register_blueprint(updater_bp)
    app.register_blueprint(journal_bp)

    return app

