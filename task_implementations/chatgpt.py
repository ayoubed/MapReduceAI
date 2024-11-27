from openai import OpenAI
from task import RetryPolicy

class ChatGPTRetryPolicy(RetryPolicy):
    def __init__(self, **kwargs):
        super().__init__(
            max_retries=3,
            initial_delay=1.0,
            max_delay=10.0,
            backoff_factor=2.0,
            jitter=True
        )
        
    def should_retry(self, error: Exception) -> bool:
        retryable_errors = (
            "rate limit",
            "timeout",
            "server error",
            "connection error",
            "500",
            "502",
            "503",
            "504"
        )
        error_msg = str(error).lower()
        return any(msg in error_msg for msg in retryable_errors)

class ChatGPTClient:
    def __init__(self):
        self.client = OpenAI(api_key="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")
        
    def call_api(self, system_prompt: str, user_prompt: str) -> str:
        response = self.client.chat.completions.create(
            model="gpt-3.5-turbo",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ]
        )
        return response.choices[0].message.content