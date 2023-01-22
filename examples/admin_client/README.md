# Admin Client

Franz-go comes with a high-level admin package (`kadm`) that provides many helper types
and functions which make cluster administration easy.

## Examples

```go
seeds := []string{"localhost:9092"}

var adminClient *kadm.Client
{
    client, err := kgo.NewClient(
        kgo.SeedBrokers(seeds...),

        // Do not try to send requests newer than 2.4.0 to avoid breaking changes in the request struct.
        // Sometimes there are breaking changes for newer versions where more properties are required to set.
        kgo.MaxVersions(kversion.V2_4_0()),
    )
    if err != nil {
        panic(err)
    }
    defer client.Close()

    adminClient = kadm.NewClient(client)
}


apiVersions, err := adminClient.ApiVersions(context.Background())
if err != nil {
    panic(err)
}

if len(apiVersions) > 0 {
    versions := apiVersions[0]
    fmt.Printf("Guessed Kafka version is: %v\n", versions.VersionGuess())
}
```