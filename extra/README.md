This directory contains various examples that are related to this project.

# Content

For repositories stored on S3 with Cloudfront CDN:

* **aws_lambda.Dockerfile**: Sample container image to run Repoup using AWS Lambda.
* **aws_lambda_edge_origin_request**: URL rewrite sample to redirect RPM request from
  _releasever_ values like `7Server` to `7`.
  Implemented as an Lambda@Edge origin request function.
* **aws_lambda_edge_origin_response**: Sample that provides a simple browsable 
  repository HTML view. Implemented as an Lambda@Edge origin response function.
