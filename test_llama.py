import os
from dotenv import load_dotenv
from langchain_ollama import ChatOllama


load_dotenv()

# 1. Initialisation de l'IA via OLLAMA (modèle local)
# Utilisation du modèle llama3 installé localement
llm = ChatOllama(
    model="llama3",  # Nom du modèle OLLAMA installé
    temperature=0,
    base_url="http://localhost:11434"  # URL par défaut d'OLLAMA
)

def test_ollama():
    print("🔄 Connexion à OLLAMA (Llama 3 Local) en cours...")
    try:
        # Test du modèle local
        res = llm.invoke("Réponds par 'OLLAMA IS READY' si tu reçois ce message.")
        print("-" * 40)
        print(f"✅ Réponse de l'Agent Local : {res.content}")
        print("-" * 40)
        print("🎯 Modèle OLLAMA prêt pour votre projet OLAP !")
    except Exception as e:
        print(f"❌ Erreur OLLAMA : {e}")
        print("💡 Vérifiez que OLLAMA est bien lancé avec: ollama serve")

if __name__ == "__main__":
    test_ollama()
