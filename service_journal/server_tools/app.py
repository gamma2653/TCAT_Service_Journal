from flask import Flask, request

try:
    from ..cmd import run_days
    from ... import service_journal as sj
    from .views.updater import updater_bp
    from ..utilities.debug import get_default_logger
except ImportError:
    from service_journal.cmd import run_days
    from service_journal.server_tools.views.updater import updater_bp
    from service_journal.utilities.debug import get_default_logger
    import service_journal as sj

logger = get_default_logger(__name__)

def create_app():
    app = Flask(__name__)

    @app.route('/', methods=['GET', 'POST'])
    def index():
        if request.method == 'GET':
            logger.info('GET request received')
            return {
                'status': 'Ready',
                'version': sj.__version__
            }
        elif request.method == 'POST':
            logger.info('POST request received')
            try:
                cmd = request.form['cmd']
                date_range = request.form['date_range']
            except IndexError:
                return 'Missing command parameter.', 400
            return {
                'status': f'Ready. Command: {cmd}, date_range: {date_range}',
                'version': sj.__version__,
            }
        else:
            return 'Invalid request method.', 405
    
    app.register_blueprint(updater_bp)

    return app

