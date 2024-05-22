from questdb.ingress import Sender, TimestampNanos

def insertQuestDB(data):
    conf = f'http::addr=localhost:9000;'
    with Sender.from_conf(conf) as sender:
        sender.row(
            'stock',
            symbols={'id': 'toronto1'},
            columns={'price': data},
            at=TimestampNanos.now())
        sender.flush()  