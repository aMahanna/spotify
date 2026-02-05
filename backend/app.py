"""Flask application factory."""

from __future__ import annotations

from flask import Flask
from flask_cors import CORS

from api import routes


def create_app() -> Flask:
    app = Flask(__name__)
    CORS(app)
    app.register_blueprint(routes.bp)
    return app
