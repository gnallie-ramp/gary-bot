from __future__ import annotations

from typing import Optional

from config import BOOKING_LINK, OWNER_NAME


def build_signature(
    name: Optional[str] = None,
    booking_link: Optional[str] = None,
    user_id: Optional[str] = None,
) -> str:
    """Build an HTML email signature.

    Parameters are resolved in this order:
    1. Explicit name/booking_link arguments
    2. Per-user lookup from registry (if user_id provided)
    3. Config defaults (OWNER_NAME, BOOKING_LINK)
    """
    sig_name = name
    sig_booking = booking_link

    if user_id and (not sig_name or not sig_booking):
        try:
            from core.user_registry import get_user_first_name, get_user_booking_link, get_user
            user = get_user(user_id)
            if user:
                if not sig_name:
                    sig_name = user.get("sf_owner_name", "") or user.get("first_name", "")
                if not sig_booking:
                    sig_booking = get_user_booking_link(user_id)
        except Exception:
            pass

    if not sig_name:
        sig_name = OWNER_NAME
    if not sig_booking:
        sig_booking = BOOKING_LINK or ""

    return f'''
<div style="font-family:Arial,sans-serif;font-size:14px;">
  <div style="font-weight:bold;font-size:15px;margin-bottom:2px;">{sig_name}</div>
  <div style="font-size:14px;color:#333;margin-bottom:4px;">Account Manager @ Ramp</div>
  <div style="font-size:13px;color:#444;margin-bottom:4px;font-style:italic;">If time-sensitive, please call Support at 1-855-206-7283, chat <a href="https://ramp.com/chat" style="color:#1155CC;font-style:italic;">here</a>, or submit a support ticket <a href="https://app.ramp.com/support/get-help?k_is=opl&q_mailing_7TUwnLRio5bqoBbU1vuPzGZXCYyTNekKfvuJH=Rp3hiHxU17BpUoBvS3UPmh287hAEn8qQEDLNiMfNdTHVDz5Vb4H8FeS1z&utm_id=YmVuQGtydW13aWVkZXJvb2ZpbmcuY29t" style="color:#1155CC;font-style:italic;">here</a></div>
  <div style="font-size:14px;margin-bottom:4px;"><a href="{sig_booking}" style="color:#1155CC;text-decoration:none;">Book a meeting</a></div>
  <div style="font-size:13px;color:#333;">21 W 23rd Street Fl 2 NY, NY 10010</div>
</div>
'''


# Default signature for backward compatibility
SIGNATURE_HTML = build_signature()
