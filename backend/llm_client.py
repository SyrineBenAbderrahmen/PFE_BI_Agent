
import json
from groq import Groq
from config import settings

client = Groq(api_key=settings.GROQ_API_KEY)

def call_llm_json(system_prompt: str, user_prompt: str, temperature: float = 0.3) -> dict:
    completion = client.chat.completions.create(
        model=settings.GROQ_MODEL,
        temperature=temperature,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )

    content = completion.choices[0].message.content.strip()

    try:
        return json.loads(content)
    except Exception:
        return {
            "help_message": "Le prompt semble encore vague ou incomplet.",
            "guided_questions": [
                "Quelle mesure veux-tu analyser exactement ?",
                "Selon quelle dimension veux-tu afficher le résultat ?",
                "Veux-tu ajouter une période ?"
            ],
            "suggested_measures": [],
            "suggested_dimensions": [],
            "reasoning_label": "fallback_json_parse_error"
        }