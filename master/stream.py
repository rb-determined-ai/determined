import lomond
from lomond import events

url = "http://localhost:8080"

# steal determined token
from determined.experimental import client
client.login(url)
token = client._determined._session._auth.session.token

ws = lomond.WebSocket(f"{url}/stream")
ws.add_header(b"Authorization", f"Bearer {token}".encode('utf8'))
for event in ws.connect():
    if isinstance(event, events.Binary):
        print(event.data.decode('utf8'))
    elif isinstance(event, events.Text):
        print(event.text.strip())
    elif isinstance(event, events.Ready):
        print("ready")
        ws.send_binary(b'{"add": {"trials": {"trial_ids": [1]}}}')
    elif isinstance(event, (events.ConnectFail, events.Rejected, events.ProtocolError)):
        raise Exception(f"connection failed: {event}")
    elif isinstance(event, (events.Closing, events.Disconnected)):
        break
