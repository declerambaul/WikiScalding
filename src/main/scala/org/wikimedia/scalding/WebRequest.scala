package org.wikimedia.scalding


trait WebRequest

// domain model for a web request
case class WebRequestText(
  hostname: String,
  sequence: String,
  dt: String,
  time_firstbyte: String,
  ip: String,
  cache_status: String,
  http_status: String,
  response_size: String,
  http_method: String,
  uri_host: String,
  uri_path: String,
  uri_query: String,
  content_type: String,
  referer: String,
  x_forwarded_for: String,
  user_agent: String,
  accept_language: String,
  x_analytics: String) extends WebRequest