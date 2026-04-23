import os
from dotenv import load_dotenv
from langchain_groq import ChatGroq

load_dotenv()

# 1. Initialisation de l'IA via Groq
# On utilise llama3-8b-8192 (très rapide et puissant pour le code)
llm = ChatGroq(
    temperature=0, 
    groq_api_key=os.getenv("GROQ_API_KEY"), 
    model_name="llama-3.1-8b-instant"
)

def test_groq():
    print(" Connexion à Groq (Llama 3) en cours...")
    try:
        # On teste l'IA
        res = llm.invoke("Réponds par 'GROQ IS READY' si tu reçois ce message.")
        print("-" * 30)
        print(f" Réponse de l'Agent : {res.content}")
    except Exception as e:
        print(f"❌ Erreur Groq : {e}")

if __name__ == "__main__":
    test_groq()