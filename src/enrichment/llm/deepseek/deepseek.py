import os
from datetime import datetime
from typing import List

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
client = OpenAI(
    api_key=os.getenv("API_KEY_DEEPSEEK"), base_url="https://api.deepseek.com"
)

SYS_MSG_GET_TITLE = """
Your prompt will be a snapshot of a text file.
You will Return only the title of the file provided to you as an answer.
Do not provide anything else. If you can not determine a title answer with "CANT_FIND_TITLE"
"""


def get_title_from_text(text: str) -> str:
    return _call_deepseek(SYS_MSG_GET_TITLE, text)


def get_title_from_text_concurrent(text: List[str]) -> List[str]:
    return _call_deepseek_concurrent(SYS_MSG_GET_TITLE, text)


def _call_deepseek(system_message: str, prompt: str):
    response = client.chat.completions.create(
        model="deepseek-chat",
        messages=[
            {"role": "system", "content": system_message},
            {"role": "user", "content": prompt},
        ],
        stream=False,
    )
    return response.choices[0].message.content


def _call_deepseek_concurrent(system_message: str, prompts: List[str]):
    start_time = datetime.now()
    batch_messages = []
    for prompt in prompts:
        batch_messages.append(
            [
                {"role": "system", "content": system_message},
                {"role": "user", "content": prompt},
            ]
        )
    responses = [
        client.chat.completions.create(model="deepseek-reasoner", messages=msg)
        for msg in batch_messages
    ]
    print(f"Batch processing Deepseek Call took {datetime.now() - start_time} seconds ")
    return [responses[idx].choices[0].message.content for idx in range(len(prompts))]
