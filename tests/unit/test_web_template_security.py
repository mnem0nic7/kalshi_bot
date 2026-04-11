from pathlib import Path


def test_room_template_avoids_inner_html_for_transcript_messages() -> None:
    room_script = Path("src/kalshi_bot/web/static/room.js").read_text(encoding="utf-8")

    assert ".innerHTML" not in room_script
    assert "body.textContent = message.content" in room_script
