import asyncio
import pandas as pd
import streamlit as st
from gql import Client, gql
from gql.transport.websockets import WebsocketsTransport

# Your Bitquery API token
TOKEN = "ory_at_Vlas3tQQ3k2dXZN7F6_JwlE5CzE-8p3oZd1-gGOZYOs.HYOuVK6s-09pzQ03kKNnZTmHpf9ZCXnD8zaXa8a88Hg"

# List of Solana addresses to monitor
addresses = [
                            "7Ppgch9d4XRAygVNJP4bDkc7V6htYXGfghX4zzG9r4cH", 
                            "G6xptnrkj4bxg9H9ZyPzmAnNsGghSxZ7oBCL1KNKJUza",
                            "BmanDqQELF7QY5uRPw8vg3eMjR74WZnszKmNbLJKkay3", 
                            "8aHVf4T3t4Z2kjnNojLh87zswhH21EU2iFnVoadN3MXx", 
                            "BQ72nSv9f3PRyRKCBnHLVrerrv37CYTHm5h3s9VSGQDV", 
                            "6LXutJvKUw8Q5ue2gCgKHQdAN4suWW8awzFVC6XCguFx",
                            "GGztQqQ6pCPaJQnNpXBgELr5cs3WwDakRbh1iEMzjgSJ", 
                            "4xDsmeTWPNjgSVSS1VTfzFq3iHZhp77ffPkAmkZkdu71",
                            "CapuXNQoDviLvU1PxFiizLgPNQCxrsag1uMeyk6zLVps", 
                            "2MFoS3MPtvyQ4Wh4M9pdfPjz6UhVoNbFbGJAskCPCj3h",
                            "9nnLbotNTcUhvbrsA6Mdkx45Sm82G35zo28AqUvjExn8", 
                            "6U91aKa8pmMxkJwBCfPTmUEfZi6dHe7DcFq2ALvB2tbB",
                            "3CgvbiM3op4vjrrjH2zcrQUwsqh5veNVRjFCB9N6sRoD", 
                            "69yhtoJR4JYPPABZcSNkzuqbaFbwHsCkja1sP1Q2aVT5",
                            "HFqp6ErWHY6Uzhj8rFyjYuDya2mXUpYEk8VW75K9PSiY", 
                            "7iWnBRRhBCiNXXPhqiGzvvBkKrvFSWqqmxRyu9VyYBxE",
                            "GP8StUXNYSZjPikyRsvkTbvRV1GBxMErb59cpeCJnDf1", 
                            "HU23r7UoZbqTUuh3vA7emAGztFtqwTeVips789vqxxBw",
                            "DSN3j1ykL3obAVNv7ZX49VsFCPe4LqzxHnmtLiPwY6xg", 
                            "3LoAYHuSd7Gh8d7RTFnhvYtiTiefdZ5ByamU42vkzd76",
                            "HV1KXxWFaSeriyFvXyx48FqG9BoFbfinB8njCJonqP7K", 
                            "FkdDHrM8j8psKbxuwjV1jBKCM2JGPygkj7WCX8sSCzNm",
                            "JD1dHSqYkrXvqUVL8s6gzL1yB7kpYymsHfwsGxgwp55h", 
                            "JD25qVdtd65FoiXNmR89JjmoJdYk9sjYQeSTZAALFiMy",
                            "JD38n7ynKYcgPpF7k1BhXEeREu1KqptU93fVGy3S624k", 
                            "5d1qPrAdqjDPt6gM4SCQdgj43kMEVhQK94XGTt9ByFWm",
                            "LLnrZq5d4cGxFXo7EhoWVSuSYcmrN2oPfoPzcV29RuG", 
                            "MfDuWeqSHEqTFVYZ7LoexgAK9dxk7cy4DFJWjWMGVWa",
                            "7bHjLgxFYdkaTExXcHUReGQDzEoUL1cxLFt2wXEUrG9p", 
                            "a7exPKwRV1rZeGWghf66jzPM1NTcANgM1sse3cmGHCp",
                            "7NdnPvcQ3hbyDivBkkzhG52ovE5rqZ4PPRChp5qu9ytN", 
                            "YubQzu18FDqJRyNfG8JqHmsdbxhnoQqcKUHBdUkN6tP",
                            "CUvt6KLpufP2SBBYTDB5dDuNu4BDS9ZQqdFAFiV91hD9", 
                            "7QUH6cSyMvMbZ1yqUai61Drjg4iScnuzH33nitRzSTW8",
                            "3gpBP2UKzkbLH5JezrUWutwx7E5JWj2mpcX4E8Krz6XD", 
                            "4GfJwy2H3rfb1wCUypoqvHZDGpuNCxbnYePuv5Tm4ptB",
                            "6kTTXRy1uecYvPEejA4F4yGakVupCRMds9xwXRwJXRwD", 
                            "EP2EsSu3dKmtJ2AxGEEMsYgzeXC2LDSnJ3SJeMBJSqoC",
                            "HAyWXoM7heQR2PreN3DPpFfSMo337banGJX75nXkWFBx", 
                            "HfQLZjVmysQZbFPRAcpwUChzKq6XiwCWaEDnQqms8ZMe",
                            "7NTk7owuYi2MJyCYmG1PyojKcA1yeTTUKJzHsybmvieM", 
                            "2XdLUoFgPgWNyryLBnDK93rr4WKAjqBetUUEwouUsUM8",
                            "GBgyWtivdmarj2ABj3Ut1qmb3QHmdwFJKh3r32JCyB3U", 
                            "HrtrAX73xv6GXusjts1xLZvEypt6EUnT3KKh2L9GmAzA",
                            "BEMBuBH38wmt69nQ3e88NKjzXpuLmYoan6kP4HXzMhnL", 
                            "1g8Lqi4mWNjNfByMjDhFK5T4JN52Z3Uaam5DPy2oMsM",
                            "5bj7rjhKZYiGVW4sJCzMyKeJpQy1s5R8uMvV4WkZbEd", 
                            "7AiBnBK7FPfq1VR1zyndPNWoCz6uYB7hA9EnZ5ZzcGgz",
                            "HjKHfDgjtCrbzg24AcfDa4eH5Bi6ZammvgHXSYkwVPmc", 
                            "DogvUMSSzFizUMBVJiYQcgwow757TDvWFn7R2ShaUb7V",
                            "CFvtCJaNhiDdUWCLCeFw6AAiY6Yfhus7EcjXRWfzZ7EM", 
                            "HWiSWt38XdnmPAiT1UhTH8uZLtwnUzsirntDvG8zijaR",
                            "F9rjfoq3YAz1e7Sn2Q5p64u9BTosqQEvjFNg8RgnqBT2", 
                            "DoBmC79Du9zMHwwYBNYrC711QSzYub2VYypskjjngoJa",
                            "3RVZ8aE8MVWfiLuvb1dyq943Mcm7ykizYKPme3CbveW9", 
                            "GKM7w468Bhse8e8HkZmAj6roJgDC5G636rfmF3P1kaSD",
                            "HZgNsuXAq7jFo5efGULhjxxTZQCXVax7K4ZixL1f8Ycm", 
                            "3RL8V3HvmuZbAi8CvsALk6rGJGYRWVuECQZ3jCpvsi27",
                            "3tzyrrpLopCnnkSYJN5YKN5Jq6hZuKAjTg6CXLgFjfbk", 
                            "AGkQVoqRB6cUzUjQmGBN1qDhF6HrXpm43EULGCVkRFwN",
                            "EXZdYstUjD7NEeamfBshwg8hdiEK4Bjj8BcvXMf4VSdD", 
                            "BMaP3z3MTcW14VQk1FPVQEZPGYmUCFZGkEyP3FpnriSp",
                            "6NVomBM5LJmfkMhKDH6BJrXD8xCJAta4wS4L44xba22J", 
                            "D7WhxKduXk2YPX7b1SZ7uPEYKrxxex71a45mXRLF3hfy",
                            "5qwymUAhV7YfwbAncwka53jzJ7ToJgcaKAUhBZS4sHY1", 
                            "3gJNV7VLcYEUKPhZKfSUpnogz1u7Z9yDLCFXTWhBkdam",
                            "2UEUPjZhpYHU5sBGXbSB5Q7zzWjQxwimfzZzgPT14Vnz", 
                            "FedzDJ8dzGmdHTrAqYgQJ6DEhnwJ1Q9e9dHrwP7fK8Nt",
                            "6rY8yJwFFnRMJpAsRmpiA3URNwnca6tZm4beEkBDAYkK", 
                            "J2CetfY4jAnBJrfE2rkpFJ4SpQ1N7tiRQ2D3QaumhbBG",
                            "24wrejMDrxMnTcCQPacFFt4eKFb8NtvYrSeRGdpfJCnb", 
                            "D7BQQaL5i3pFKFcKZUN39ZtZRiLNcpE5HU5arDUN6uy7",
                            "MJKqp326RZCHnAAbew9MDdui3iCKWco7fsK9sVuZTX2",  
                            "52C9T2T7JRojtxumYnYZhyUmrN7kqzvCLc4Ksvjk7TxD",
                            "8BseXT9EtoEhBTKFFYkwTnjKSUZwhtmdKY2Jrj8j45Rt",  
                            "GitYucwpNcg6Dx1Y15UQ9TQn8LZMX1uuqQNn8rXxEWNC",
                            "9QgXqrgdbVU8KcpfskqJpAXKzbaYQJecgMAruSWoXDkM", 
                            "9uRJ5aGgeu2i3J98hsC5FDxd2PmRjVy9fQwNAy7fzLG3",
                            "EJRJswH9LyjhAfBWwPBvat1LQtrJYK4sVUzsea889cQt",
                            "53nHsQXkzZUp5MF1BK6Qoa48ud3aXfDFJBbe1oECPucC",
                            "8PjJTv657aeN9p5R2WoM6pPSz385chvTTytUWaEjSjkq",
                            "AHB94zKUASftTdqgdfiDSdnPJHkEFp7zX3yMrcSxABsv",
                            "3D91zLQPRLamwJfGR5ZYMKQb4C18gsJNaSdmB6b2wLhw",
                            "AogcwQ1ubM76EPMhSD5cw1ES4W5econvQCFmBL6nTW1",
                            "3bHbMa5VW3np5AJazuacidrN4xPZgwhcXigmjwHmBg5e",
                            "AYgECURrvuX6GtFe4tX7aAj87Xc5r5Znx96ntNk1nCv",
                            "Hc36Wh1ZqYGzGAnsJWNT9r2gY3h9n89uDpxZPsmEsiE3",
                            "9UGxCidmZtU1PM7Tbhv2twQ8ChsS6S3HdL1xo56fSVWn",
                            "9hyH1vCWstfPxPuoBJX6dYb3HxyzHSN765ZSVfqVBfFZ",
                            "y1ZtQhHkfcKvoPvmVhLie7KZsZLFZfNZqvgQGY9TZt1",
                            "7GE3Y6CjpKKKbuP3XMhLgK5KyjBcRcCy6sT6oEFtkQ7L",
                            "39nLeBVHx9vStTDMJ2zWJUwvif1xe2jXxaTjkobRmCUF",
                            "GxyRKP2eVKACaSSnso4VLSAjZKmHsFXHWUfS3A5CtiMA",
                            "FbGeZS8LiPCZiFpFwdUUeF2yxXtSsdfJoHTsVMvM8STh",
                            "C8BSJt7GVYZNpkLTM9rHFzh1VQm14bYQPQFS77NSSugG",
                            "9oua4J9GGEzHUqCyqiQs3D6rf5ZouwNzm8S2Hm415q9B",
                            "4rF6k3aRX54yhHakGDEbwvPzb36d2PKw688VqchP3bNU",
                            "BifEkWRt8sZnpKkuh4nThqK8VJxUprfKwuq6uXBDFmcW",
                            "8MyWcZGvLKWcnePJCdgE7aZEt3Bf7ttM7RkCRCyDtYMi",
                            "5MPGAb9xjdpscnRFBnp4ZCGJrndU6JmMrmbJKqSaLn9i",
                            "8yJ5J9ZrNQNErD3xDRebp7jwxbKU1vLtY5Yn11wv4r3X",
                            "2Ls7ywRZTSeViL17DKZCoBZVb1u9oB7jyjowqqCNaxvq",
                            "AE99TFcM9DHRRLTxBZjJxLQ1Gdo89S9yyDAU8WUEmPpb",
                            "9prVdaYZTtH1LDbZZHdKrvSqTD1WM4q4NNMMWgzn4rE1",
                            "Db3z2bUaQy6NS8iMKPikEALS248Rt1WAzR3kRL82XDRD",
                            "4XCJ5PbHJWP1xfBKyYmV6GnUox1KY1czCiBQW1U3NCNj",
                            "9WX4J4EnJXq5PX4gjdDEuQ6tAZUivjjMZrdGiZcG7Nmf",
                            "H6vpvhyv8nVeXsoE3GCyZ4q2EViENnzwTJzw5fe8LnFV",
                            "GYj8DfqHfDacQvhQbstvZMRWduUMu81ptLU7RwvYxgG6",
                            "3UhvFW6b3QoUtJ51TG1QhneYxWJ8sEYUhrchrna3SEzq",
                            "8Htve3nXPsvXk88WrJHH6nQBQCjw4bSCJLuEpT6ArfMY",
                            "CKCPbSG7Zre8mN8Xar9NoEMjeKfozJ7Lip14cSv2BTHR",
                            "4JvDtKc7cy3pyi2SSfXTtHy1XJbSX8hcZfUKRwC9s2HZ",
                            "DrG3SskFqoXyLL6Y2gP54DvBxm27jYWXfefyRCM6ikKB",
                            "7vPT2dc2tD72vqWyFDGiTxsRZQyFFmmSanGRHV6Ywi9t",
                            "CVgyXrbEd1ctEuvq11QdpnCQVnPit8NLdhyqXQHLprM2",
                            "6Y7JiG5QF87B8WL7TJCRVBkccS9K57SwMTHUHMDqiK8J",
                            "CAq7MnR96g6b9nncVk7HDsgaNLHaYt4hUmq2VvWhdcoF",
                            "8wipZfKKzwfXBcYrqEtndjCjy9XHkoET1NB4NkbcwWLV",
                            "3hqfpZ775LMcG5WUrMCQLFeY1ABRnCYD2zr2UYaQSBVF",
                            "BTYBjYjodGY7K2ifq1c4Wv4WMbJQPVN7tUStTsfcvR31",
                            "FwqnvVsMQ9Txv9FWy23imxN2Rd6jL6WJcsGNDBgQvucR",
                            "ABpLdyDkJzR5eBpF7cR3H8NUV4NtN6nn6kidDpxAXqNm",
                            "9XcVGH2iKSGz2LU3nubpPT56PXq4hZiL33RwuwPX2X4k",
                            "91QTxnQ1rR59Z7XyzvYe3qYuD69uBdaLUUfD5GFbR3wr",
                            "67AUyQHdnt5vD7pZV7JuH5ySyRDvNHRTZaGF15qxXVnH",
                            "2LFuenHsgr7nk5CrDsUpEZFiTsjwsUJgGyYzYZzPB6hZ",
                            "J5y8QgfSRNTxbpPdfjJkq6tWC6zqYgsR4wy1S17McDHN",
                            "C4q71UEMVQqmVFgMPWMPzrmh1jAZcZA8xd2aeUU5gtr5",
                            "6fAvoivLhaAt2voohbUwPYQ29y3g7pY9ar7XgtZJEWyv",
                            "45MdcQ2sX1ceRuTrThyePoWeRUXNk3GPzrvTHm4gcJju",
                            "4xAM3b4ReNCTgRgGvQtoAgKkcS6nZsfJDpAgc1U9XKzg"
]

# Create the subscription query to monitor transfers
def create_transfer_subscription_query():
    address_list = ', '.join(f'"{addr}"' for addr in addresses)
    return gql(f"""
        subscription {{
            Solana {{
                Transfers(
                    where: {{any: [{{
                        Transfer: {{Sender: {{Address: {{in: [{address_list}]}}}}}}
                    }}]}}
                ) {{
                    Transfer {{
                        Amount
                        AmountInUSD
                        Receiver {{
                            Address
                        }}
                        Sender {{
                            Address
                        }}
                        Currency {{
                            Symbol
                        }}
                    }}
                }}
            }}
        }}
    """)

# Function to monitor transfers for specified addresses
async def monitor_transfers():
    transport = WebsocketsTransport(
        url=f"wss://streaming.bitquery.io/eap?token={TOKEN}",
        headers={"Sec-WebSocket-Protocol": "graphql-transport-ws"}
    )
    
    await transport.connect()
    print("Connected to WebSocket for transfer monitoring.")

    transfers_df = pd.DataFrame()

    # Set up Streamlit display for transfers
    st.title("Solana Transfer Monitoring Dashboard")
    table = st.empty()  # Initialize an empty Streamlit table
    
    try:
        query = create_transfer_subscription_query()
        async for result in transport.subscribe(query):
            if result.data:
                print("Transfer Data:", result.data)  # Debugging output
                transfer_data = pd.json_normalize(result.data['Solana']['Transfers'])
                
                # Select only the relevant fields
                display_data = transfer_data[[
                    'Transfer.Sender.Address',
                    'Transfer.Receiver.Address',
                    'Transfer.Amount',
                    'Transfer.AmountInUSD',
                    'Transfer.Currency.Symbol'
                ]]
                
                transfers_df = pd.concat([transfers_df, display_data], ignore_index=True)

                # Update the Streamlit table with the new data
                with st.spinner('Updating data...'):
                    table.table(transfers_df)
                    
    except Exception as e:
        print("Error during transfer monitoring:", e)
    finally:
        await transport.close()

def main():
    asyncio.run(monitor_transfers())

if __name__ == "__main__":
    main()
