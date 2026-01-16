"""
Tests for GraphRAG chat interface.

Tests the LLM synthesis function with different models to ensure
parameter compatibility.
"""

from unittest.mock import Mock


def test_gpt_5_2_parameters():
    """Test that GPT-5.2 models use correct parameters."""
    from scripts.chat_graphrag import synthesize_answer

    # Mock OpenAI client
    mock_client = Mock()
    mock_response = Mock()
    mock_response.choices = [Mock()]
    mock_response.choices[0].message.content = "Test answer"
    mock_client.chat.completions.create.return_value = mock_response

    # Test GPT-5.2 model
    result = synthesize_answer(
        client=mock_client,
        question="Test question",
        context="Test context",
        companies=[("AAPL", "Apple Inc.")],
        traversal_paths=[],
        conversation_history=[],
        model="gpt-5.2-chat-latest",
    )

    # Verify the API was called
    assert mock_client.chat.completions.create.called

    # Get the call arguments
    call_kwargs = mock_client.chat.completions.create.call_args[1]

    # Verify GPT-5.2 specific parameters
    assert call_kwargs["model"] == "gpt-5.2-chat-latest"
    assert "max_completion_tokens" in call_kwargs
    assert call_kwargs["max_completion_tokens"] == 2000
    assert "temperature" not in call_kwargs  # GPT-5.2 doesn't support custom temperature
    assert "max_tokens" not in call_kwargs  # Should use max_completion_tokens instead

    assert result == "Test answer"


def test_gpt_4o_parameters():
    """Test that older models (GPT-4o) use correct parameters."""
    from scripts.chat_graphrag import synthesize_answer

    # Mock OpenAI client
    mock_client = Mock()
    mock_response = Mock()
    mock_response.choices = [Mock()]
    mock_response.choices[0].message.content = "Test answer"
    mock_client.chat.completions.create.return_value = mock_response

    # Test GPT-4o model
    result = synthesize_answer(
        client=mock_client,
        question="Test question",
        context="Test context",
        companies=[("AAPL", "Apple Inc.")],
        traversal_paths=[],
        conversation_history=[],
        model="gpt-4o",
    )

    # Verify the API was called
    assert mock_client.chat.completions.create.called

    # Get the call arguments
    call_kwargs = mock_client.chat.completions.create.call_args[1]

    # Verify GPT-4o parameters
    assert call_kwargs["model"] == "gpt-4o"
    assert "max_tokens" in call_kwargs
    assert call_kwargs["max_tokens"] == 2000
    assert "temperature" in call_kwargs
    assert call_kwargs["temperature"] == 0.3
    assert "max_completion_tokens" not in call_kwargs  # Should use max_tokens instead

    assert result == "Test answer"


def test_gpt_4_1_mini_parameters():
    """Test that GPT-4.1-mini uses correct parameters."""
    from scripts.chat_graphrag import synthesize_answer

    # Mock OpenAI client
    mock_client = Mock()
    mock_response = Mock()
    mock_response.choices = [Mock()]
    mock_response.choices[0].message.content = "Test answer"
    mock_client.chat.completions.create.return_value = mock_response

    # Test GPT-4.1-mini model
    result = synthesize_answer(
        client=mock_client,
        question="Test question",
        context="Test context",
        companies=[("AAPL", "Apple Inc.")],
        traversal_paths=[],
        conversation_history=[],
        model="gpt-4.1-mini",
    )

    # Verify the API was called
    assert mock_client.chat.completions.create.called

    # Get the call arguments
    call_kwargs = mock_client.chat.completions.create.call_args[1]

    # Verify GPT-4.1-mini parameters
    assert call_kwargs["model"] == "gpt-4.1-mini"
    assert "max_tokens" in call_kwargs
    assert call_kwargs["max_tokens"] == 2000
    assert "temperature" in call_kwargs
    assert call_kwargs["temperature"] == 0.3

    assert result == "Test answer"


def test_error_handling():
    """Test that errors are handled gracefully."""
    from scripts.chat_graphrag import synthesize_answer

    # Mock OpenAI client that raises an error
    mock_client = Mock()
    mock_client.chat.completions.create.side_effect = Exception("API Error")

    result = synthesize_answer(
        client=mock_client,
        question="Test question",
        context="Test context",
        companies=[],
        traversal_paths=[],
        conversation_history=[],
        model="gpt-5.2-chat-latest",
    )

    assert "Error generating answer" in result
    assert "API Error" in result
