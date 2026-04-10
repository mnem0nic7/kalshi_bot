from pathlib import Path


def test_room_template_avoids_inner_html_for_transcript_messages() -> None:
    template = Path("src/kalshi_bot/web/templates/room.html").read_text(encoding="utf-8")

    assert "article.innerHTML" not in template
    assert "body.textContent = message.content" in template
