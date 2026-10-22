import unittest
from contextlib import ExitStack
from unittest.mock import patch

import export_to_excel as export


class MinimumBatchTests(unittest.TestCase):
    def test_zero_or_one_row_has_no_delivery_or_state_changes(self):
        for count in (0, 1):
            with self.subTest(count=count), ExitStack() as stack:
                stack.enter_context(patch.object(export, 'configure_logging'))
                stack.enter_context(patch.object(export, 'load_dotenv'))
                stack.enter_context(patch.dict(export.os.environ, {
                    'TELEGRAM_BOT_TOKEN_ASSISTANT': 'test',
                    'TELEGRAM_CHAT_ID': 'test',
                }))
                stack.enter_context(patch.object(
                    export, 'fetch_new_rows_with_state',
                    return_value=([('phone', 'utm', 'direction', 'status')] * count, {}, []),
                ))
                mocks = [stack.enter_context(patch.object(export, name)) for name in (
                    'save_client_csv', 'send_document_with_retries',
                    'send_email_with_attachment_with_retries', 'send_text_message',
                    'store_last_ids', 'save_tariff_remaining', 'mark_rows_as_sent',
                )]

                export.main()

                for mock in mocks:
                    mock.assert_not_called()

    def test_two_rows_reach_file_creation(self):
        rows = [('phone', 'utm', 'direction', 'status')] * 2
        with (
            patch.object(export, 'configure_logging'),
            patch.object(export, 'load_dotenv'),
            patch.dict(export.os.environ, {
                'TELEGRAM_BOT_TOKEN_ASSISTANT': 'test',
                'TELEGRAM_CHAT_ID': 'test',
            }),
            patch.object(export, 'fetch_new_rows_with_state', return_value=(rows, {}, [])),
            patch.object(export, 'save_client_csv', side_effect=RuntimeError('stop before delivery')) as save,
        ):
            with self.assertRaisesRegex(RuntimeError, 'stop before delivery'):
                export.main()
            self.assertEqual(save.call_args.args[0], rows)


if __name__ == '__main__':
    unittest.main()
