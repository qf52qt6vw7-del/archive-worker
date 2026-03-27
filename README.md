# archive-worker

Background worker that drains accepted rows from `email_send_queue`
into `email_send_history` using the archive RPC flow.
