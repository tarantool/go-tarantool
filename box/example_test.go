// Run Tarantool Common Edition before example execution:
//
// Terminal 1:
// $ cd box
// $ TEST_TNT_LISTEN=127.0.0.1:3013 tarantool testdata/config.lua
//
// Terminal 2:
// $ go test -v example_test.go

package box_test

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/tarantool/go-tarantool/v3"
	"github.com/tarantool/go-tarantool/v3/box"
)

func ExampleBox_Info() {
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	cancel()
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// You can use Info Request type.

	fut := client.Do(box.NewInfoRequest())

	resp := &box.InfoResponse{}

	err = fut.GetTyped(resp)
	if err != nil {
		log.Fatalf("Failed get box info: %s", err)
	}

	// Or use simple Box implementation.

	b := box.MustNew(client)

	info, err := b.Info()
	if err != nil {
		log.Fatalf("Failed get box info: %s", err)
	}

	if info.UUID != resp.Info.UUID {
		log.Fatalf("Box info uuids are not equal")
	}

	fmt.Printf("Box info uuids are equal\n")
	fmt.Printf("Current box ro: %+v", resp.Info.RO)
	// Output:
	// Box info uuids are equal
	// Current box ro: false
}

func ExampleSchemaUser_Exists() {
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx := context.Background()

	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})

	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// You can use UserExistsRequest type and call it directly.
	fut := client.Do(box.NewUserExistsRequest("user"))

	resp := &box.UserExistsResponse{}

	err = fut.GetTyped(resp)
	if err != nil {
		log.Fatalf("Failed get box schema user exists with error: %s", err)
	}

	// Or use simple User implementation.
	b := box.MustNew(client)

	exists, err := b.Schema().User().Exists(ctx, "user")
	if err != nil {
		log.Fatalf("Failed get box schema user exists with error: %s", err)
	}

	if exists != resp.Exists {
		log.Fatalf("Box schema users exists are not equal")
	}

	fmt.Printf("Box schema users exists are equal\n")
	fmt.Printf("Current exists state: %+v", exists)
	// Output:
	// Box schema users exists are equal
	// Current exists state: false
}

func ExampleSchemaUser_Create() {
	// Connect to Tarantool.
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx := context.Background()

	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// Create SchemaUser.
	schemaUser := box.MustNew(client).Schema().User()

	// Create a new user.
	username := "new_user"
	options := box.UserCreateOptions{
		IfNotExists: true,
		Password:    "secure_password",
	}
	err = schemaUser.Create(ctx, username, options)
	if err != nil {
		log.Fatalf("Failed to create user: %s", err)
	}

	fmt.Printf("User '%s' created successfully\n", username)
	// Output:
	// User 'new_user' created successfully
}

func ExampleSchemaUser_Drop() {
	// Connect to Tarantool.
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx := context.Background()

	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// Create SchemaUser.
	schemaUser := box.MustNew(client).Schema().User()

	// Drop an existing user.
	username := "new_user"
	options := box.UserDropOptions{
		IfExists: true,
	}
	err = schemaUser.Drop(ctx, username, options)
	if err != nil {
		log.Fatalf("Failed to drop user: %s", err)
	}

	exists, err := schemaUser.Exists(ctx, username)
	if err != nil {
		log.Fatalf("Failed to get user exists: %s", err)
	}

	fmt.Printf("User '%s' dropped successfully\n", username)
	fmt.Printf("User '%s' exists status: %v \n", username, exists)
	// Output:
	// User 'new_user' dropped successfully
	// User 'new_user' exists status: false
}

func ExampleSchemaUser_Password() {
	// Connect to Tarantool.
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx := context.Background()

	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// Create SchemaUser.
	schemaUser := box.MustNew(client).Schema().User()

	// Get the password hash.
	password := "my-password"
	passwordHash, err := schemaUser.Password(ctx, password)
	if err != nil {
		log.Fatalf("Failed to get password hash: %s", err)
	}

	fmt.Printf("Password '%s' hash: %s", password, passwordHash)
	// Output:
	// Password 'my-password' hash: 3PHNAQGFWFo0KRfToxNgDXHj2i8=
}

func ExampleSchemaUser_Info() {
	// Connect to Tarantool.
	dialer := tarantool.NetDialer{
		Address:  "127.0.0.1:3013",
		User:     "test",
		Password: "test",
	}
	ctx := context.Background()

	client, err := tarantool.Connect(ctx, dialer, tarantool.Opts{})
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	// Create SchemaUser.
	schemaUser := box.MustNew(client).Schema().User()

	info, err := schemaUser.Info(ctx, "test")
	if err != nil {
		log.Fatalf("Failed to get password hash: %s", err)
	}

	hasSuper := false
	for _, i := range info {
		if i.Name == "super" && i.Type == box.PrivilegeRole {
			hasSuper = true
		}
	}

	if hasSuper {
		fmt.Printf("User have super privileges")
	}
	// Output:
	// User have super privileges
}
