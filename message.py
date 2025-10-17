
type2number = {
    "choke": 0,
    "unchoke": 1,
    "interested": 2,
    "not interested": 3,
    "have": 4,
    "bitfield": 5,
    "request": 6,
    "piece": 7,
    "header": 8
}

number2type = {v: k for k, v in type2number.items()}

def string2hex(string):
    ''.join(format(ord(char), '02x') for char in string)
class Message:
    type = "header"
    payload = ""
    trueMSG = [] #an array with the true byes (hexadecimal
    def __init__(self, type, payload=""):
        trueMSG = [0 for _ in range(64)]
        type = type2number[type]

        if type == "header": #create a correct msg
            hexHead = string2hex("P2PFILESHARINGPROJ")
            for i , val in enumerate(hexHead):
                trueMSG[i] = val

                #next 10 bytes are zero

                #TODO: figure out how peerID is set

        if(payload != ""): #none empty payload, make sure its not too long
            if(type == 0 or type == 1 or type == 2 or type == 3): #these don't have payloads
                raise ValueError("Type: " + str(number2type(type)) + " cannot have a payload")

