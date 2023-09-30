import json

def load_secrets():
    with open('commit_secrets.json') as f:
        com_secrets = json.load(f)
    return com_secrets