# Security

tsession is a **test-environment** tool. It records request/response payloads and data mutations, and session
bindings attribute traffic without authenticating it (JWT claims are decoded, not verified). Do not run it against
production traffic or real customer data, and keep every component on trusted networks.

To report a vulnerability, please open a private security advisory on the GitHub repository rather than a public issue.
