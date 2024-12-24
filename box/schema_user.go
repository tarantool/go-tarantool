package box

import (
	"context"
	"fmt"
	"strings"

	"github.com/tarantool/go-tarantool/v2"
	"github.com/vmihailenco/msgpack/v5"
)

// SchemaUser provides methods to interact with schema-related user operations in Tarantool.
type SchemaUser struct {
	conn tarantool.Doer // Connection interface for interacting with Tarantool.
}

// NewSchemaUser creates a new SchemaUser instance with the provided Tarantool connection.
// It initializes a SchemaUser object, which provides methods to perform user-related
// schema operations (such as creating, modifying, or deleting users) in the Tarantool instance.
func NewSchemaUser(conn tarantool.Doer) *SchemaUser {
	return &SchemaUser{conn: conn}
}

// UserExistsRequest represents a request to check if a user exists in Tarantool.
type UserExistsRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// UserExistsResponse represents the response to a user existence check.
type UserExistsResponse struct {
	Exists bool // True if the user exists, false otherwise.
}

// DecodeMsgpack decodes the response from a Msgpack-encoded byte slice.
func (uer *UserExistsResponse) DecodeMsgpack(d *msgpack.Decoder) error {
	arrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	// Ensure that the response array contains exactly 1 element (the "Exists" field).
	if arrayLen != 1 {
		return fmt.Errorf("protocol violation; expected 1 array entry, got %d", arrayLen)
	}

	// Decode the boolean value indicating whether the user exists.
	uer.Exists, err = d.DecodeBool()

	return err
}

// NewUserExistsRequest creates a new request to check if a user exists.
func NewUserExistsRequest(username string) UserExistsRequest {
	callReq := tarantool.NewCallRequest("box.schema.user.exists").Args([]interface{}{username})

	return UserExistsRequest{
		callReq,
	}
}

// Exists checks if the specified user exists in Tarantool.
func (u *SchemaUser) Exists(ctx context.Context, username string) (bool, error) {
	// Create a request and send it to Tarantool.
	req := NewUserExistsRequest(username).Context(ctx)
	resp := &UserExistsResponse{}

	// Execute the request and parse the response.
	err := u.conn.Do(req).GetTyped(resp)

	return resp.Exists, err
}

// UserCreateOptions represents options for creating a user in Tarantool.
type UserCreateOptions struct {
	// IfNotExists - if true, prevents an error if the user already exists.
	IfNotExists bool `msgpack:"if_not_exists"`
	// Password for the new user.
	Password string `msgpack:"password"`
}

// UserCreateRequest represents a request to create a new user in Tarantool.
type UserCreateRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewUserCreateRequest creates a new request to create a user with specified options.
func NewUserCreateRequest(username string, options UserCreateOptions) UserCreateRequest {
	callReq := tarantool.NewCallRequest("box.schema.user.create").
		Args([]interface{}{username, options})

	return UserCreateRequest{
		callReq,
	}
}

// UserCreateResponse represents the response to a user creation request.
type UserCreateResponse struct{}

// DecodeMsgpack decodes the response for a user creation request.
// In this case, the response does not contain any data.
func (uer *UserCreateResponse) DecodeMsgpack(_ *msgpack.Decoder) error {
	return nil
}

// Create creates a new user in Tarantool with the given username and options.
func (u *SchemaUser) Create(ctx context.Context, username string, options UserCreateOptions) error {
	// Create a request and send it to Tarantool.
	req := NewUserCreateRequest(username, options).Context(ctx)
	resp := &UserCreateResponse{}

	// Execute the request and handle the response.
	fut := u.conn.Do(req)

	err := fut.GetTyped(resp)
	if err != nil {
		return err
	}

	return nil
}

// UserDropOptions represents options for dropping a user in Tarantool.
type UserDropOptions struct {
	IfExists bool `msgpack:"if_exists"` // If true, prevents an error if the user does not exist.
}

// UserDropRequest represents a request to drop a user from Tarantool.
type UserDropRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewUserDropRequest creates a new request to drop a user with specified options.
func NewUserDropRequest(username string, options UserDropOptions) UserDropRequest {
	callReq := tarantool.NewCallRequest("box.schema.user.drop").
		Args([]interface{}{username, options})

	return UserDropRequest{
		callReq,
	}
}

// UserDropResponse represents the response to a user drop request.
type UserDropResponse struct{}

// Drop drops the specified user from Tarantool, with optional conditions.
func (u *SchemaUser) Drop(ctx context.Context, username string, options UserDropOptions) error {
	// Create a request and send it to Tarantool.
	req := NewUserDropRequest(username, options).Context(ctx)
	resp := &UserCreateResponse{}

	// Execute the request and handle the response.
	fut := u.conn.Do(req)

	err := fut.GetTyped(resp)
	if err != nil {
		return err
	}

	return nil
}

// UserPasswordRequest represents a request to retrieve a user's password from Tarantool.
type UserPasswordRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewUserPasswordRequest creates a new request to fetch the user's password.
// It takes the username and constructs the request to Tarantool.
func NewUserPasswordRequest(username string) UserPasswordRequest {
	// Create a request to get the user's password.
	callReq := tarantool.NewCallRequest("box.schema.user.password").Args([]interface{}{username})

	return UserPasswordRequest{
		callReq,
	}
}

// UserPasswordResponse represents the response to the user password request.
// It contains the password hash.
type UserPasswordResponse struct {
	Hash string // The password hash of the user.
}

// DecodeMsgpack decodes the response from Tarantool in Msgpack format.
// It expects the response to be an array of length 1, containing the password hash string.
func (upr *UserPasswordResponse) DecodeMsgpack(d *msgpack.Decoder) error {
	// Decode the array length.
	arrayLen, err := d.DecodeArrayLen()
	if err != nil {
		return err
	}

	// Ensure the array contains exactly 1 element (the password hash).
	if arrayLen != 1 {
		return fmt.Errorf("protocol violation; expected 1 array entry, got %d", arrayLen)
	}

	// Decode the string containing the password hash.
	upr.Hash, err = d.DecodeString()

	return err
}

// Password sends a request to retrieve the user's password from Tarantool.
// It returns the password hash as a string or an error if the request fails.
// It works just like hash function.
func (u *SchemaUser) Password(ctx context.Context, password string) (string, error) {
	// Create the request and send it to Tarantool.
	req := NewUserPasswordRequest(password).Context(ctx)
	resp := &UserPasswordResponse{}

	// Execute the request and handle the response.
	fut := u.conn.Do(req)

	// Get the decoded response.
	err := fut.GetTyped(resp)
	if err != nil {
		return "", err
	}

	// Return the password hash.
	return resp.Hash, nil
}

// UserPasswdRequest represents a request to change a user's password in Tarantool.
type UserPasswdRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewUserPasswdRequest creates a new request to change  a user's password in Tarantool.
func NewUserPasswdRequest(args ...string) (UserPasswdRequest, error) {
	callReq := tarantool.NewCallRequest("box.schema.user.passwd")

	switch len(args) {
	case 1:
		callReq.Args([]interface{}{args[0]})
	case 2:
		callReq.Args([]interface{}{args[0], args[1]})
	default:
		return UserPasswdRequest{}, fmt.Errorf("len of fields must be 1 or 2, got %d", len(args))

	}

	return UserPasswdRequest{callReq}, nil
}

// UserPasswdResponse represents the response to a user passwd request.
type UserPasswdResponse struct{}

// Passwd sends a request to set a password for a currently logged in or a specified user.
// A currently logged-in user can change their password using box.schema.user.passwd(password).
// An administrator can change the password of another user
// with box.schema.user.passwd(username, password).
func (u *SchemaUser) Passwd(ctx context.Context, args ...string) error {
	req, err := NewUserPasswdRequest(args...)
	if err != nil {
		return err
	}

	req.Context(ctx)

	resp := &UserPasswdResponse{}

	// Execute the request and handle the response.
	fut := u.conn.Do(req)

	err = fut.GetTyped(resp)
	if err != nil {
		return err
	}

	return nil
}

// UserInfoRequest represents a request to get a user's info in Tarantool.
type UserInfoRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewUserInfoRequest creates a new request to get user privileges.
func NewUserInfoRequest(username string) UserInfoRequest {
	callReq := tarantool.NewCallRequest("box.schema.user.info").Args([]interface{}{username})

	return UserInfoRequest{
		callReq,
	}
}

// PrivilegeType is a struct based on privilege object types list
// https://www.tarantool.io/en/doc/latest/admin/access_control/#all-object-types-and-permissions
type PrivilegeType string

const (
	// PrivilegeUniverse - privilege type based on universe.
	// A database (box.schema) that contains database objects, including spaces,
	// indexes, users, roles, sequences, and functions.
	// Granting privileges to universe gives a user access to any object in the database.
	PrivilegeUniverse PrivilegeType = "universe"
	// PrivilegeTypeUser - privilege type based on user.
	// A user identifies a person or program that interacts with a Tarantool instance.
	PrivilegeTypeUser PrivilegeType = "user"
	// PrivilegeRole - privilege type based on role.
	// A role is a container for privileges that can be granted to users.
	// Roles can also be assigned to other roles, creating a role hierarchy.
	PrivilegeRole PrivilegeType = "role"
	// PrivilegeSpace - privilege type based on space.
	// Tarantool stores tuples in containers called spaces.
	PrivilegeSpace PrivilegeType = "space"
	// PrivilegeFunction - privilege type based on functions.
	// This allows access control based on function access.
	PrivilegeFunction PrivilegeType = "function"
	// PrivilegeSequence - privilege type based on sequences.
	// A sequence is a generator of ordered integer values.
	PrivilegeSequence PrivilegeType = "sequence"
	// PrivilegeLuaEval - privilege type based on executing arbitrary Lua code.
	PrivilegeLuaEval PrivilegeType = "lua_eval"
	// PrivilegeLuaCall - privilege type based on
	// calling any global user-defined Lua function.
	PrivilegeLuaCall PrivilegeType = "lua_call"
	// PrivilegeSQL - privilege type based on
	// executing an arbitrary SQL expression.
	PrivilegeSQL PrivilegeType = "sql"
)

// Permission is a struct based on permission tarantool object
// https://www.tarantool.io/en/doc/latest/admin/access_control/#permissions
type Permission string

const (
	// PermissionRead allows reading data of the specified object.
	// For example, this permission can be used to allow a user
	// to select data from the specified space.
	PermissionRead Permission = "read"
	// PermissionWrite allows updating data of the specified object.
	// For example, this permission can be used to allow
	// a user to modify data in the specified space.
	PermissionWrite Permission = "write"
	// PermissionCreate allows creating objects of the specified type.
	// For example, this permission can be used to allow a user to create new spaces.
	// Note that this permission requires read and write access to certain system spaces.
	PermissionCreate Permission = "create"
	// PermissionAlter allows altering objects of the specified type.
	// Note that this permission requires read and write access to certain system spaces.
	PermissionAlter Permission = "alter"
	// PermissionDrop allows dropping objects of the specified type.
	// Note that this permission requires read and write access to certain system spaces.
	PermissionDrop Permission = "drop"
	// PermissionExecute for role,
	// allows using the specified role. For other object types, allows calling a function.
	// Can be used only for role, universe, function, lua_eval, lua_call, sql.
	PermissionExecute Permission = "execute"
	// PermissionSession allows a user to connect to an instance over IPROTO.
	PermissionSession Permission = "session"
	// PermissionUsage allows a user to use their privileges on database objects
	// (for example, read, write, and alter spaces).
	PermissionUsage Permission = "usage"
)

// Privilege is a structure that is used to create new rights,
// as well as obtain information for rights.
type Privilege struct {
	// Permissions is a list of privileges that apply to the privileges object type.
	Permissions []Permission
	// Type - one of privilege object types (it might be space,function, etc.).
	Type PrivilegeType
	// Name - can be the name of a function or space,
	// and can also be empty in case of universe access
	Name string
}

// UserInfoResponse represents the response to a user info request.
type UserInfoResponse struct {
	Privileges []Privilege
}

// DecodeMsgpack decodes the response from Tarantool in Msgpack format.
func (uer *UserInfoResponse) DecodeMsgpack(d *msgpack.Decoder) error {
	rawArr := make([][][3]string, 0)

	err := d.Decode(&rawArr)
	if err != nil {
		return err
	}

	privileges := make([]Privilege, len(rawArr[0]))

	for i, rawPrivileges := range rawArr[0] {
		strPerms := strings.Split(rawPrivileges[0], ",")

		perms := make([]Permission, len(strPerms))
		for j, strPerm := range strPerms {
			perms[j] = Permission(strPerm)
		}

		privileges[i] = Privilege{
			Permissions: perms,
			Type:        PrivilegeType(rawPrivileges[1]),
			Name:        rawPrivileges[2],
		}
	}

	uer.Privileges = privileges

	return nil
}

// Info returns a list of user privileges according to the box.schema.user.info method call.
func (u *SchemaUser) Info(ctx context.Context, username string) ([]Privilege, error) {
	req := NewUserInfoRequest(username).Context(ctx)

	resp := &UserInfoResponse{}
	fut := u.conn.Do(req)

	err := fut.GetTyped(resp)
	if err != nil {
		return nil, err
	}

	return resp.Privileges, nil
}

// prepareGrantAndRevokeArgs prepares the arguments for granting or revoking user permissions.
// It accepts a username, a privilege, and options for either granting or revoking.
// The generic type T can be UserGrantOptions or UserRevokeOptions.
func prepareGrantAndRevokeArgs[T UserGrantOptions | UserRevokeOptions](username string,
	privilege Privilege, opts T) []interface{} {

	args := []interface{}{username} // Initialize args slice with the username.

	switch privilege.Type {
	case PrivilegeUniverse:
		// Preparing arguments for granting permissions at the universe level.
		// box.schema.user.grant(username, permissions, 'universe'[, nil, {options}])
		strPerms := make([]string, len(privilege.Permissions))
		for i, perm := range privilege.Permissions {
			strPerms[i] = string(perm) // Convert each Permission to a string.
		}

		reqPerms := strings.Join(strPerms, ",") // Join permissions into a single string.

		// Append universe-specific arguments to args.
		args = append(args, reqPerms, string(privilege.Type), nil, opts)
	case PrivilegeRole:
		// Handling the case where the object type is a role name.
		// Append role-specific arguments to args.
		args = append(args, privilege.Name, nil, nil, opts)
	default:
		// Preparing arguments for granting permissions on a specific object.
		strPerms := make([]string, len(privilege.Permissions))
		for i, perm := range privilege.Permissions {
			strPerms[i] = string(perm) // Convert each Permission to a string.
		}

		reqPerms := strings.Join(strPerms, ",") // Join permissions into a single string.
		// box.schema.user.grant(username, permissions, object-type, object-name[, {options}])
		// Example: box.schema.user.grant('testuser', 'read', 'space', 'writers')
		args = append(args, reqPerms, string(privilege.Type), privilege.Name, opts)
	}

	return args // Return the prepared arguments.
}

// UserGrantOptions holds options for granting permissions to a user.
type UserGrantOptions struct {
	Grantor     string `msgpack:"grantor,omitempty"` // Optional grantor name.
	IfNotExists bool   `msgpack:"if_not_exists"`     // Option to skip if the grant already exists.
}

// UserGrantRequest wraps a Tarantool call request for granting user permissions.
type UserGrantRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// NewUserGrantRequest creates a new UserGrantRequest based on provided parameters.
func NewUserGrantRequest(username string, privilege Privilege,
	opts UserGrantOptions) UserGrantRequest {
	args := prepareGrantAndRevokeArgs[UserGrantOptions](username, privilege, opts)

	// Create a new call request for the box.schema.user.grant method with the given args.
	callReq := tarantool.NewCallRequest("box.schema.user.grant").Args(args)

	return UserGrantRequest{callReq} // Return the UserGrantRequest.
}

// UserGrantResponse represents the response from a user grant request.
type UserGrantResponse struct{}

// Grant executes the user grant operation in Tarantool, returning an error if it fails.
func (u *SchemaUser) Grant(ctx context.Context, username string, privilege Privilege,
	opts UserGrantOptions) error {
	req := NewUserGrantRequest(username, privilege, opts).Context(ctx)

	resp := &UserGrantResponse{} // Initialize a response object.
	fut := u.conn.Do(req)        // Execute the request.

	err := fut.GetTyped(resp) // Get the typed response and check for errors.
	if err != nil {
		return err // Return any errors encountered.
	}

	return nil // Return nil if the operation was successful.
}

// UserRevokeOptions holds options for revoking permissions from a user.
type UserRevokeOptions struct {
	IfExists bool `msgpack:"if_exists"` // Option to skip if the revoke does not exist.
}

// UserRevokeRequest wraps a Tarantool call request for revoking user permissions.
type UserRevokeRequest struct {
	*tarantool.CallRequest // Underlying Tarantool call request.
}

// UserRevokeResponse represents the response from a user revoke request.
type UserRevokeResponse struct{}

// NewUserRevokeRequest creates a new UserRevokeRequest based on provided parameters.
func NewUserRevokeRequest(username string, privilege Privilege,
	opts UserRevokeOptions) UserRevokeRequest {
	args := prepareGrantAndRevokeArgs[UserRevokeOptions](username, privilege, opts)

	// Create a new call request for the box.schema.user.revoke method with the given args.
	callReq := tarantool.NewCallRequest("box.schema.user.revoke").Args(args)

	return UserRevokeRequest{callReq}
}

// Revoke executes the user revoke operation in Tarantool, returning an error if it fails.
func (u *SchemaUser) Revoke(ctx context.Context, username string, privilege Privilege,
	opts UserRevokeOptions) error {
	req := NewUserRevokeRequest(username, privilege, opts).Context(ctx)

	resp := &UserRevokeResponse{} // Initialize a response object.
	fut := u.conn.Do(req)         // Execute the request.

	err := fut.GetTyped(resp) // Get the typed response and check for errors.
	if err != nil {
		return err
	}

	return nil
}
