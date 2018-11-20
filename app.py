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
from stellar_base.builder import Builder
import base64

import datetime
from threading import Thread
import numpy as np
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

def getColdwalletaddr(cc):
    db = getMONGO()
    crypt = db["configs"].find_one({"cc": cc})
    if (crypt == None):
        return None
    return crypt["coldwallet"]

def getKeys(cc):
    db = getMONGO()
    crypt = db["configs"].find_one({"cc":cc})
    if(crypt == None):
        return None
    keys = crypt["keys"]
    sp = keys.split(",")
    return {"private":sp[1],"public":sp[0]}

def getConfig(cc,key):
    db = getMONGO()
    crypt = db["configs"].find_one({"cc":cc})
    if(crypt == None):
        return None
    return crypt[key]

def getBTCRPC():
    return RPCHost(getConfig("bitcoin","rpcurl"))


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
        keys = getKeys("ripple")

        rem = RippleRemote(getConfig("ripple","socket"), keys["private"])
        return jsonify(rem.account_info(keys["public"]))
    except Exception as e:
        return jsonify({"error":str(e)})

@app.route('/stellar/userbalance')
@requires_auth
def Stellar_UserBalance():
    try:
        horizon = Horizon(horizon_uri=getConfig("stellar","horizon"))
        keys = getKeys("stellar")
        ac = horizon.account(keys["public"])
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
        },
        {
            'id': 'BitcoinColdWalletTransfer',
            'func': 'app:BitcoinColdWalletTransfer',
            'args': None,
            'trigger': 'interval',
            'hours': 2
        },
        {
            'id': 'RippleColdWalletTransfer',
            'func': 'app:RippleColdWalletTransfer',
            'args': None,
            'trigger': 'interval',
            'hours': 2
        },
        {
            'id': 'StellarColdWalletTransfer',
            'func': 'app:StellarColdWalletTransfer',
            'args': None,
            'trigger': 'interval',
            'hours': 2
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

def log(op, type, detail):
    o = {"operation":op,"type":type,"time":datetime.datetime.now(),"detail":detail}
    db = getMONGO()
    db["wallet_history"].insert_one(o)



def StellarColdWalletTransfer():
    try:
        horizon = Horizon(horizon_uri=getConfig("stellar","horizon"))
        keys = getKeys("stellar")

        ac = horizon.account(keys["public"])
        balances = ac["balances"]

        balance = np.float64(0.0)
        transfer = np.float64(0.0)

        for b in balances:
            if(b["asset_type"] == "native"):
                balance = np.float64( b["balance"] )
        print("stellar balance : " + str(balance))
        if(balance < np.float64(2000.000000)):
            return
        transfer = balance - np.float64(1000.000000)
        log("STELLAR_CD_TRANSFER", "INFO", {"totalbalance": str(balance), "transferbalance": str(transfer)})
        builder = Builder(secret=keys["private"], horizon_uri=getConfig("stellar","horizon"), network='TESTNET')
        builder.add_text_memo("DVZHWALLET").append_payment_op(destination=getColdwalletaddr("stellar"), amount=str(transfer), asset_code='XLM')
        builder.sign()
        response = builder.submit()
    except Exception as e:
        log("STELLAR_CD_TRANSFER", "ERROR", e)
def RippleColdWalletTransfer():
    try:
        keys = getKeys("ripple")
        rem = RippleRemote(getConfig("ripple","socket"), keys["private"])
        balance = rem.account_info(keys["public"])["Balance"]
        if(np.uint64(balance) < np.uint64(2000000000)):
            return

        transfer = np.uint64(balance) - np.uint64(1000000000)
        log("RIPPLE_CD_TRANSFER", "INFO", {"totalbalance": balance, "transferbalance": str(transfer)})
        #print("ripple transfer: " + str(transfer))
        payment = rem.send_payment(getColdwalletaddr("ripple"), transfer,None,None,"10001" )


    except Exception as e:
        log("RIPPLE_CD_TRANSFER", "ERROR", e)

def BitcoinColdWalletTransfer():
    #lets get btc hw balance
    try:
        host = getBTCRPC()
        tbalance = host.call('getbalance')
        if(tbalance < 0.20000000):
            return
        balance = tbalance - 0.10000000
        log("BTC_CD_TRANSFER","INFO",{"totalbalance":tbalance,"transferbalance":balance})

        send = host.call('sendtoaddress',getColdwalletaddr("bitcoin"),balance)
        log("BTC_CD_TRANSFER", "INFO", {"totalbalance": tbalance, "transferbalance": balance,"sendresult":send})



    except Exception as e:
        log("BTC_CD_TRANSFER", "ERROR", e)


def StellarPaymentListener():
    #NO SSE For version Alpha
    db = getMONGO()
    #TODO: Check is config exist or throw something bad
    config = db["configs"].find_one({"cc":"stellar"})
    keys = getKeys("stellar")

    address = Address(address=keys["public"],
                      horizon_uri=getConfig("stellar","horizon"))
    horizon = Horizon(horizon_uri=getConfig("stellar","horizon"))

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
        if(sto != keys["public"]):
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
    keys = getKeys("ripple")
    ws.send("{\"id\": \"DVZHotWalletStream\",\"command\": [\"subscribe\"],\"accounts\": [\""+keys["public"]+"\"],\"streams\":[\"transactions\"]}")


def RippleWS():
    global ripplews
    websocket.enableTrace(True)
    ripplews = websocket.WebSocketApp(getConfig("ripple","socket"),
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
