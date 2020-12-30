import base64
from google.cloud import firestore
import json


def pubsub_to_firestore(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
     Args:
          event (dict): Event payload.
          context (google.cloud.functions.Context): Metadata for the event.
     """

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')

    print(pubsub_message)

    # Add a new document
    db = firestore.Client()
    doc_ref = db.collection(u'iotapp').document(u'device-id')

    doc = doc_ref.get()

    data_json = json.loads(pubsub_message)
    record = {}

    if bool(doc.to_dict()):

        record = doc.to_dict()
        record["live"] = data_json

        if len(record["set"]) == 10:
            record["set"].pop(0)
            record["set"].append(data_json)
        else:
            record["set"].append(data_json)
    else:

        record = {
            "live": data_json,
            "set": []
        }

    doc_ref.set(record)

    print("END")
