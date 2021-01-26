# Admin Requests

All Kafka requests and responses are supported through generated code in the
kmsg package. The package aims to provide some relatively comprehensive
documentation (at least more than Kafka itself provides), but may be lacking
in some areas.

If you are using the kmsg package manually, it is **strongly** recommended to
set the MaxVersions option so that you do not accidentally have new fields
added across client versions erroneously have zero value defaults sent to
Kafka.

It is recommended to always set all fields of a request. If you are talking
to a broker that does not support all fields you intend to use, those fields
are silently not written to that broker. It is recommended to ensure your
brokers support what you are expecting to send them.

To issue a kmsg request, use the client's `Request` function. This function
is a bit overpowered, as specified in its documentation.