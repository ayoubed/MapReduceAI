import logging

from task_implementations.chatgpt import ChatGPTClient, ChatGPTRetryPolicy
from typing import Dict, Any
from task import Task

class TranslationTask(Task):
    """Task for translating text using ChatGPT"""
    def __init__(self, target_language: str, **kwargs):
        kwargs['retry_policy'] = kwargs.get('retry_policy', ChatGPTRetryPolicy())
        super().__init__(**kwargs)
        self.target_language = target_language
        self.client = ChatGPTClient()
        
    def execute(self, inputs: Dict[str, Any]) -> Dict[str, Any]:
        parsed_content = inputs[list(inputs.keys())[0]]["parsed_content"]
        
        system_prompt = f"""
        Translate the following text to {self.target_language}.
        Maintain the original structure and formatting.
        """
        
        try:
            result = self.client.call_api(system_prompt, parsed_content)
            return {"translated_content": result}
        except Exception as e:
            logging.error(f"Error translating text: {str(e)}")
            raise