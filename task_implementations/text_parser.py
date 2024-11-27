import logging

from task_implementations.chatgpt import ChatGPTClient, ChatGPTRetryPolicy
from typing import Dict, Any
from task import Task

class TextParsingTask(Task):
    """Task for parsing text using ChatGPT"""
    def __init__(self, text: str, **kwargs):
        kwargs['retry_policy'] = kwargs.get('retry_policy', ChatGPTRetryPolicy())
        super().__init__(**kwargs)
        self.text = text
        self.client = ChatGPTClient()
        
    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        system_prompt = """
        Analyze the given text and extract:
        1. Main topics
        2. Key points
        3. Overall sentiment
        Return the analysis as a structured text.
        """
        
        try:
            result = self.client.call_api(system_prompt, self.text)
            return {"parsed_content": result}
        except Exception as e:
            logging.error(f"Error parsing text: {str(e)}")
            raise