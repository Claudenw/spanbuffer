A SpanBuffer is a virtual byte buffer that has Span attributes.  This package provides mechanisms to create
buffers from multiples sources and then merge them together to make new virtual buffers.  The original buffers
are not duplicated but rather are referenced by later buffers.

Input and output stream classes are provided.  This makes this package particularly suited for passing large 
objects over smaller buffers as with MQTT or Kafka messaging systems.

SpanBuffers are also suited for use in diff/patch applications where buffers are intermingled.


