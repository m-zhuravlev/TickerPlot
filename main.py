import threading
import time
from collections import deque
from datetime import datetime
from random import random

import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import plotly.graph_objs as go
from dash.dependencies import Input, Output, State
from plotly import tools

DATE_MASK = "%Y/%m/%d, %H:%M:%S"

PERIOD_SEC = 1
queue = deque()
table = []

app = dash.Dash(
    __name__, meta_tags=[{"name": "viewport", "content": "width=device-width"}],
)

app.title = "Ticker plot"

server = app.server

tickers = [i for i in range(0, 100)]


# consumer = KafkaConsumer('ticker-quote',
#                          group_id='0',
#                          bootstrap_servers=['localhost:9092'],
#                          value_deserializer=lambda v: json.loads(v.decode('utf-8'))
#                          )


def tickers_options():
    return list(map(lambda t: {"label": "Ticker_{:0>2}".format(t), "value": str(t)}, tickers))


# Dash App Layout
app.layout = html.Div(
    className="row",
    children=[
        # Interval component for graph updates
        dcc.Interval(id="i_tris", interval=1 * 2000, n_intervals=0),

        #
        html.Div(
            children=[
                # Graph div
                html.Div(
                    dcc.Graph(
                        id="chart",
                        className="chart-graph",
                        config={"displayModeBar": True, "scrollZoom": True},
                    )
                ),
                html.Div(
                    dcc.Dropdown(
                        id="dropdown_ticker",
                        className="dropdown-ticker",
                        options=tickers_options(),
                        value="1",
                        clearable=False,
                    )
                ),
            ],
        ),
    ],
)


def get_fig(ticker):
    data_ser = pd.Series(table[int(ticker) + 1], table[0])
    df = data_ser.resample('10S').ohlc()

    fig = tools.make_subplots(
        rows=1,
        shared_xaxes=True,
        shared_yaxes=True,
        cols=1,
        print_grid=False,
        vertical_spacing=0.12,
    )

    fig.append_trace(colored_bar_trace(df), 1, 1)

    fig["layout"][
        "uirevision"
    ] = "The User is always right"  # Ensures zoom on graph is the same on update
    fig["layout"]["margin"] = {"t": 50, "l": 50, "b": 50, "r": 25}
    fig["layout"]["autosize"] = True
    fig["layout"]["height"] = 400
    fig["layout"]["xaxis"]["rangeslider"]["visible"] = False
    fig["layout"]["xaxis"]["tickformat"] = "%H:%M:%S"
    fig["layout"]["yaxis"]["showgrid"] = True
    fig["layout"]["yaxis"]["gridcolor"] = "#3E3F40"
    fig["layout"]["yaxis"]["gridwidth"] = 1
    fig["layout"].update(paper_bgcolor="#21252C", plot_bgcolor="#21252C")

    return fig


def colored_bar_trace(df):
    return go.Ohlc(
        x=df.index,
        open=df["open"],
        high=df["high"],
        low=df["low"],
        close=df["close"],
        showlegend=False,
        name="colored bar",
    )


# Callback to update the actual graph
def generate_figure_callback():
    def chart_fig_callback(n_i, ticker, ask):

        if ticker is None:
            return {"layout": {}, "data": {}}

        if int(ticker) not in tickers:
            return {"layout": {}, "data": []}

        fig = get_fig(ticker)
        return fig

    return chart_fig_callback


app.callback(
    Output("chart", "figure"),
    [
        Input("i_tris", "n_intervals"),
        Input("dropdown_ticker", "value"),
    ],
    [
        State("chart", "figure"),
    ],
)(generate_figure_callback())


def generate_movement():
    movement: int = -1 if random() < 0.5 else 1
    return movement


class ThreadGenerator(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, *, daemon=None):
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self.flag = True

    def run(self):
        curr_prices = [0 for _ in range(0, 100)]
        first_row = [datetime.now()]
        first_row.extend(curr_prices)
        queue.append(first_row)
        # producer = KafkaProducer(bootstrap_servers='localhost:9092',
        #                          value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        while self.flag:
            for i in range(0, 100):
                new_price = curr_prices[i] + generate_movement()
                curr_prices[i] = new_price
            row = [datetime.now()]
            row.extend(curr_prices)
            # future = producer.send('ticker-quote', row)
            queue.append(row)
            time.sleep(PERIOD_SEC)
        print("-------------------------------------------")


class ThreadReader(threading.Thread):
    def __init__(self, group=None, target=None, name=None,
                 args=(), kwargs=None, *, daemon=None):
        super().__init__(group, target, name, args, kwargs, daemon=daemon)
        self.flag = True

    def run(self):
        # for message in consumer:
        #     time_value = datetime.datetime.fromtimestamp(message.timestamp / 1e3)
        #     print("%s %s:%d:%d: key=%s value=%s" % (time_value, message.topic, message.partition,
        #                                             message.offset, message.key,
        #                                             message.value))
        table.append([datetime.now()])
        for i in range(0, 100):
            table.append([0])

        while self.flag:
            while len(queue) > 0:
                row = queue.popleft()
                for i in range(0, 101):
                    table[i].append(row[i])
            time.sleep(PERIOD_SEC)
            print('{} -- {}'.format(table[0][-1], table[1][-1]))


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    thread1 = ThreadGenerator()
    thread1.setDaemon(True)
    thread1.start()

    thread2 = ThreadReader()
    thread2.setDaemon(True)
    thread2.start()

    app.run_server(debug=True)
