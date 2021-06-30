import os
import sys
import click
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func
from flask_restful import Resource, Api
import datetime as dt
from flask import Flask, jsonify, render_template, request, url_for, redirect, flash

from bsv.downloader import *

# Initialize (including Database)
WIN = sys.platform.startswith('win')
if WIN:  # 如果是 Windows 系统，使用三个斜线
    prefix = 'sqlite:///'
else:  # 否则使用四个斜线
    prefix = 'sqlite:////'

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = prefix + os.path.join(os.getcwd(), os.getenv('DATABASE_FILE', 'data.db'))
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False  # 关闭对模型修改的监控

db = SQLAlchemy(app)

class Holdings(db.Model):

    __tablename__ = "Holdings"
    id = db.Column(db.Integer, primary_key = True)
    tday = db.Column(db.String(8))
    exchange = db.Column(db.String(5))
    code = db.Column(db.String(2))
    product = db.Column(db.String(10))
    contract = db.Column(db.String(6))
    symbol = db.Column(db.String(1))
    rank = db.Column(db.Integer)
    member = db.Column(db.String(4))
    value = db.Column(db.Integer)
    change = db.Column(db.Integer)

    def __repr__(self):
        return '<Holding {0}: tday {1}, exchange {2}>'.format(self.id, self.tday, self.exchange)

def get_next_date(date, timedelta=1, timeformat='%Y%m%d'):
    """
    return the next day by default, format: "YYYYMMDD"
    """
    next_date = dt.datetime.strptime(date, timeformat) + dt.timedelta(days = timedelta)
    return next_date.strftime(timeformat)

today = dt.datetime.today().strftime("%Y%m%d")
this_file_path = os.path.dirname(os.path.abspath(__file__))
dce_dir = os.path.join(this_file_path, "DCE_Dir")
dce_zip = os.path.join(this_file_path, "DCE_Zip")

@app.cli.command()
@click.option('--drop', is_flag = True, help = "Create after drop")
def initdb(drop):
    """Initialize the database"""
    if drop:
        db.drop_all()
    db.create_all()
    click.echo("Initialized database!")

@app.cli.command()
@click.option("--start_date", required=True, prompt = True, help = "Enter start date, format: %Y%m%d", type = click.STRING)
@click.option("--end_date", default = today, prompt = True, help = "Enter end date, format: %Y%m%d", type=click.STRING)
@click.option("--exchange", required=True, prompt = True, help='exchange name: SHFE, CFFEX, DCE, CZCE')
def update_data(start_date, end_date, exchange):
    """
    Collect the data from start_date to end_date
    :param start_date: "YYYYMMDD"
    :param end_date: "YYYYMMDD" (defaulted: yesterday)
    """
    exchange = exchange.lower()
    if exchange == 'dce':
        if not os.path.exists(dce_dir):
            os.mkdir(dce_dir)
        if not os.path.exists(dce_zip):
            os.mkdir(dce_zip)
    _func = globals()['_'.join(['hp', exchange])]
    db.drop_all()
    db.create_all()

    while start_date <= end_date:
        if exchange == 'dce':
            _generator = _func(start_date, dce_dir, dce_zip)
        else:
            _generator = _func(start_date)
        for i in _generator:
            if not Holdings.query.filter_by(tday=i[0]).filter_by(exchange=exchange.upper()).first():
                db.session.add(Holdings(
                    tday=i[0],
                    exchange=i[1],
                    code=i[2],
                    product=i[3],
                    contract=i[4],
                    symbol=i[5],
                    rank=i[6],
                    member=i[7],
                    value=i[8],
                    change=i[9]
                ))
        db.session.commit()
        start_date = get_next_date(start_date)

class RequestExchange(Resource):

    def get(self):
        ex = list(Holdings.query.with_entities(Holdings.exchange).distinct().all()[0])
        start_date = db.session.query(func.min(Holdings.tday)).all()[0][0]
        end_date = db.session.query(func.max(Holdings.tday)).all()[0][0]
        return jsonify(status_code = 200,
                       msg = 'Success',
                       data = {'exchange': ex,
                               'start_date': start_date,
                               'end_date': end_date})


@app.route("/", methods = ["GET", "POST"])
def index():
    holdings = Holdings.query.all()
    if request.method == 'POST':
        start_date = request.form.get('start_date')
        end_date = request.form.get('end_date')
        exchange = request.form.get('exchange')

        exchange = exchange.lower()
        if exchange == 'dce':
            if not os.path.exists(dce_dir):
                os.mkdir(dce_dir)
            if not os.path.exists(dce_zip):
                os.mkdir(dce_zip)
        _func = globals()['_'.join(['hp', exchange])]
        db.drop_all()
        db.create_all()

        if not end_date:
            end_date = dt.datetime.today().strftime("%Y%m%d")

        while start_date <= end_date:
            if exchange == 'dce':
                _generator = _func(start_date, dce_dir, dce_zip)
            else:
                _generator = _func(start_date)
            for i in _generator:
                db.session.add(Holdings(
                    tday=i[0],
                    exchange=i[1],
                    code=i[2],
                    product=i[3],
                    contract=i[4],
                    symbol=i[5],
                    rank=i[6],
                    member=i[7],
                    value=i[8],
                    change=i[9]
                ))
            db.session.commit()
            start_date = get_next_date(start_date)

        holdings = Holdings.query.all()
        return redirect(url_for('index'))
    return render_template("base.html", holdings = holdings)













