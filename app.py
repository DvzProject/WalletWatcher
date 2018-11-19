import pymongo
from queue import Queue, Empty
import asyncio
import simplejson

from flask import Flask
from flask import jsonify
from bitcoinrpc.authproxy import AuthServiceProxy, JSONRPCException
from classes.RPCHost import RPCHost
from bson.decimal128 import Decimal128
from bson.json_util import dumps
from flask_apscheduler import APScheduler
from flask import request, Response
from functools import wraps
from stellar_base.address import Address
from stellar_base.horizon import Horizon
from ripple import Client as RippleClient
from ripple import Remote as RippleRemote

from threading import Thread

import websocket


global db,ripplews

app = Flask(__name__)

def check_auth(username, password):
    return username == 'test' and password == 'test'

def authenticate():
    return Response(
    'Could not verify your access level for that URL.\n'
    'You have to login with proper credentials', 401,
    {'WWW-Authenticate': 'Basic realm="Login Required"'})

def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_auth(auth.username, auth.password):
            return authenticate()
        return f(*args, **kwargs)
    return decorated




def getBTCRPC():
    rpc_user = "d4P6zbah6RY89u5f4bhJQMNvM7xb94Nt"
    rpc_password = "jV9gujb5VVPKHVzv9sDvLX27Nv5Nk8mh"
    BTCRPCURL = "http://%s:%s@46.166.173.10:18332" % (rpc_user, rpc_password)
    return RPCHost(BTCRPCURL)


def getMONGO():
    mongo = pymongo.MongoClient("mongodb://dvz:KeDDm7LVwuDnTXtHez7dLJCjR9C5QwPY@46.166.173.10/admin")
    return mongo["DVZ_HWallets"]


@app.route('/bitcointransaction/<txid>')
def bitcointransaction(txid):
    try:
        global db
        host = getBTCRPC()
        transaction = host.call('gettransaction',txid)
        txdb = db["bitcoin_history"].find_one({"txid":txid})
        if(txdb == None):
            o = {"txid": txid, "amount": transaction["amount"], "receiveaddr": transaction["details"][0]["address"],"approved": False, "confirms": transaction["confirmations"],"time": transaction["time"],"hex": transaction["hex"]}
            db["bitcoin_history"].insert_one(o)
        else:
            o = txdb

        response = app.response_class(
            response=dumps(o),
            status=200,
            mimetype='application/json'
        )
        return response
    except Exception as e:
        print(e)
        return "E",200




@app.route('/bitcoin/createAccount')
@requires_auth
def BTC_CreateAccount():
    try:
        host = getBTCRPC()
        accountaddr = host.call('getnewaddress', "useraccounts","p2sh-segwit")
        prv = host.call('dumpprivkey', accountaddr)
        return jsonify({ "public":accountaddr,"private":prv })
    except Exception as e:
        return jsonify({"error":str(e)})

@app.route('/bitcoin/userbalance')
@requires_auth
def BTC_UserBalance():
    try:
        host = getBTCRPC()
        balance = host.call('getbalance')
        return jsonify({ "balance":balance })
    except Exception as e:
        return jsonify({"error":str(e)})

@app.route('/ripple/userbalance')
@requires_auth
def Ripple_UserBalance():
    try:
        rem = RippleRemote("wss://s.altnet.rippletest.net:51233", "saDRVTqt9QRvbYZg7ukkgZKyGwzKn")
        return jsonify(rem.account_info("r9uPy68rJ214jjMerj8g3e17UR4zh4z6an"))
    except Exception as e:
        return jsonify({"error":str(e)})

@app.route('/stellar/userbalance')
@requires_auth
def Stellar_UserBalance():
    try:
        horizon = Horizon(horizon_uri="https://horizon-testnet.stellar.org")
        ac = horizon.account("GCJILL2KY3NJNZQMBONVNHJMG3CGJFONT2RQHCLTNDM53E42J6543GZV")
        return jsonify(ac)
    except Exception as e:
        return jsonify({"error":str(e)})








class Config(object):

    JOBS = [
        {
            'id': 'BTCTransactionApprover',
            'func': 'app:BTCTransactionApprover',
            'args': None,
            'trigger': 'interval',
            'seconds': 10
        },
        {
            'id': 'StellarPaymentListener',
            'func': 'app:StellarPaymentListener',
            'args': None,
            'trigger': 'interval',
            'seconds': 1
        }
    ]

    SCHEDULER_API_ENABLED = True

def updatebitcointransaction(t):
    #Modify BSON's "confirmations"
    txid = t["txid"]
    host = getBTCRPC()
    transaction = host.call('gettransaction', txid)
    t["confirms"] = transaction["confirmations"]
    return t



def StellarPaymentListener():
    #NO SSE For version Alpha
    db = getMONGO()

    #TODO: Check is config exist or throw something bad
    config = db["configs"].find_one({"cc":"stellar"})
    address = Address(address="GCJILL2KY3NJNZQMBONVNHJMG3CGJFONT2RQHCLTNDM53E42J6543GZV",
                      horizon_uri="https://horizon-testnet.stellar.org")
    horizon = Horizon(horizon_uri="https://horizon-testnet.stellar.org")

    payments = address.payments(cursor=config["cursor"])
    records = payments["_embedded"]["records"]

    cursor = config["cursor"]

    for r in records:
        type = r["type"]

        if(type != "payment"):
            continue

        asset_type = r["asset_type"]
        #We only listen XLM(native)
        if(asset_type != "native"):
            continue;


        hash = r["transaction_hash"]
        sfrom = r["from"]
        sto = r["to"]
        amount = r["amount"]
        if(sto != "GCJILL2KY3NJNZQMBONVNHJMG3CGJFONT2RQHCLTNDM53E42J6543GZV"):
            continue

        f = db["stellar_history"].find_one({"hash": hash})
        if(f != None):
            continue
        fulltrans = horizon.transaction(hash)
        senderaccount = horizon.account(sfrom)

        #This security check is not very important for Native Asset, we can use it just for Alpha!
        FoundAssetInBalances = False

        for balance in senderaccount["balances"]:
            if(balance["asset_type"] == "native"):
                FoundAssetInBalances = True

        if(FoundAssetInBalances == False):
            print("asset not found")
            continue
        o = {"hash":hash,"memo":fulltrans["memo"],"source_account":fulltrans["source_account"],"fee_paid":fulltrans["fee_paid"]}
        db["stellar_history"].insert_one(o)
        print(amount + " received from stellar")

        # TODO:Notify engine about amount!

        if(r["paging_token"] > cursor):
            cursor = r["paging_token"]
    if(cursor != config["cursor"]):
        db["configs"].update_one( {"cc":"stellar"},{"$set":{"cursor":cursor}} )





def BTCTransactionApprover():
    db = getMONGO()
    for t in db["bitcoin_history"].find({"approved": False}):
        oldconfirm = t["confirms"]
        try:
            t = updatebitcointransaction(t)
            if (int(t["confirms"]) >= 3):
                t["approved"] = True
            if (t["confirms"] != oldconfirm):
                db["bitcoin_history"].update_one({"txid": t["txid"]},{"$set": {"approved": t["approved"], "confirms": t["confirms"]}})
        except:
            continue






def Ripple_on_message(ws, message):
    global db
    transaction = simplejson.loads(message)
    if(transaction["type"] != "transaction" or transaction["validated"] != True):
        return
    info = transaction["transaction"]
    hash = info["hash"]
    o = db["stellar_history"].find_one({"hash":hash})
    if(o != None):
        return
    print(info)
    o = {"hash":hash,"from":info["Account"],"to":info["Destination"],"tag":info["DestinationTag"],"amount":info["Amount"],"fee":info["Fee"]}
    db["stellar_history"].insert_one(o)
    print("Ripple got new payment!")

    # TODO:Notify engine about amount!

def Ripple_on_error(ws, error):
    print(error)

def Ripple_on_close(ws):
    print("### closed ###")

def Ripple_on_open(ws):
    ws.send("{\"id\": \"DVZHotWalletStream\",\"command\": [\"subscribe\"],\"accounts\": [\"rnNMw7mE7w9vHNqmy4zMhU9WHtfS3vGH53\"],\"streams\":[\"transactions\"]}")


def RippleWS():
    global ripplews
    websocket.enableTrace(True)
    ripplews = websocket.WebSocketApp("wss://s.altnet.rippletest.net:51233",
                                on_message=Ripple_on_message,
                                on_error=Ripple_on_error,
                                on_close=Ripple_on_close,
                                on_open=Ripple_on_open)
    ripplews.run_forever()
if __name__ == '__main__':
    #test

   # rem = RippleRemote("wss://s.altnet.rippletest.net:51233","saDRVTqt9QRvbYZg7ukkgZKyGwzKn")

    #print(rem.account_info("r9uPy68rJ214jjMerj8g3e17UR4zh4z6an"))

   # v = rem.send_payment("rnNMw7mE7w9vHNqmy4zMhU9WHtfS3vGH53",1,None,None,12345)
    #rip = RippleClient("wss://s.altnet.rippletest.net:51233")

    # asyncio.get_event_loop().run_until_complete(RippleTransactionListener())

    #test


    db = getMONGO()
    app.config.from_object(Config())
    scheduler = APScheduler()
    scheduler.init_app(app)
    scheduler.start()

    thread = Thread(target=RippleWS)
    thread.start()


    app.run()
    ripplews.close()
