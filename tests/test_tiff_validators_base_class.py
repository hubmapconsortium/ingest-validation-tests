import re


class TestTiffValidators:
    def check_errors(self, msg_re_list, errors):
        assert len(msg_re_list) == len(errors)
        unmatched_errors = []
        msg_re_list_dup = list(msg_re_list)  # to avoid editing during iteration
        for err_str in errors:
            match = False
            for re_str in msg_re_list_dup:
                # Need to remove unmatched "None" values to avoid Exception from re
                if len([string for string in [err_str, re_str] if string is None]) == 1:
                    continue
                elif (err_str is None and re_str is None) or re.fullmatch(
                    re_str, err_str, flags=re.MULTILINE
                ):
                    msg_re_list_dup.remove(re_str)
                    match = True
                    break
            if not match:
                unmatched_errors.append(err_str)
        if unmatched_errors or msg_re_list_dup:
            raise Exception(
                f"""Unmatched errors: {unmatched_errors}
                    Expected errors not found: {msg_re_list_dup}"""
            )
