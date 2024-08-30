import sys
from flask import Flask
from azure_service_bus import receive, send
import threading

sys.path.append('/home/pyuser/app/dist-packages/')

app = Flask(__name__)
app.config["DEBUG"] = True
thread = threading.Thread(target=receive)

@app.route('/')
def greeting():
    return 'Hello World!'

@app.route('/send/<msg>')
def send_msg(msg):
    send(msg)
    return 'Successfully send %s' %msg

if __name__ == '__main__':
    print("Start to listen to ASB topic")
    thread.start()
    app.run('0.0.0.0','8080')