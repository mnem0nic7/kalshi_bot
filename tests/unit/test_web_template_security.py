from pathlib import Path


def test_room_template_avoids_inner_html_for_transcript_messages() -> None:
    room_script = Path("src/kalshi_bot/web/static/room.js").read_text(encoding="utf-8")

    assert ".innerHTML" not in room_script
    assert "body.textContent = message.content" in room_script


def test_control_room_script_avoids_inner_html_for_dashboard_rendering() -> None:
    control_room_script = Path("src/kalshi_bot/web/static/control_room.js").read_text(encoding="utf-8")

    assert ".innerHTML" not in control_room_script
    assert ".replaceChildren" in control_room_script
