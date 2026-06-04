from types import SimpleNamespace

from app.deepseek_client import call_deepseek_chat


class FakeChatCompletions:
    def create(self, model, messages, max_tokens, temperature, extra_body=None):
        return SimpleNamespace(
            model=model,
            choices=[
                SimpleNamespace(
                    message=SimpleNamespace(content="fake DeepSeek response"),
                    finish_reason="stop",
                )
            ],
            usage=SimpleNamespace(
                prompt_tokens=10,
                completion_tokens=5,
                total_tokens=15,
            ),
        )


class FakeClient:
    def __init__(self):
        self.chat = SimpleNamespace(
            completions=FakeChatCompletions()
        )


def test_call_deepseek_chat_maps_response_to_result():
    result = call_deepseek_chat(
        prompt="hello",
        client=FakeClient(),
        model="deepseek-v4-pro",
        max_tokens=64,
    )

    assert result.prompt_text == "hello"
    assert result.response_text == "fake DeepSeek response"
    assert result.model_name == "deepseek-v4-pro"
    assert result.prompt_tokens == 10
    assert result.completion_tokens == 5
    assert result.total_tokens == 15
    assert result.http_status == 200
    assert result.finish_reason == "stop"
    assert result.latency_ms >= 0
