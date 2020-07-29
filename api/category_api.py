from flask import Flask
from uwsgidecorators import postfork
from cassandra.cluster import Cluster

session = None
prepared = None

@postfork
def connect():
    global session, prepared
    session = Cluster().connect()
    prepared = session.prepare("SELECT title FROM wiki.categories WHERE category=?")


app = Flask(__name__)


@app.route('/<category>')
def get_outdated_title(category):
    title = session.execute(prepared, (category,))[0]
    return "Most outdated page for ["+category+"] category = "+title


if __name__ == '__main__':
    app.run()

