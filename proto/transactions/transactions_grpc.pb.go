// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.4.0
// - protoc             v4.25.4
// source: transactions/transactions.proto

package transactions

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.62.0 or later.
const _ = grpc.SupportPackageIsVersion8

const (
	TransactionService_WriteIntTxn_FullMethodName      = "/transactions.TransactionService/WriteIntTxn"
	TransactionService_WriteFloatTxn_FullMethodName    = "/transactions.TransactionService/WriteFloatTxn"
	TransactionService_WriteStrTxn_FullMethodName      = "/transactions.TransactionService/WriteStrTxn"
	TransactionService_WriteMapTxn_FullMethodName      = "/transactions.TransactionService/WriteMapTxn"
	TransactionService_ProcessTxnErrors_FullMethodName = "/transactions.TransactionService/ProcessTxnErrors"
	TransactionService_ReadIntTxns_FullMethodName      = "/transactions.TransactionService/ReadIntTxns"
	TransactionService_ReadFloatTxns_FullMethodName    = "/transactions.TransactionService/ReadFloatTxns"
	TransactionService_ReadStrTxn_FullMethodName       = "/transactions.TransactionService/ReadStrTxn"
	TransactionService_ReadMapTxn_FullMethodName       = "/transactions.TransactionService/ReadMapTxn"
)

// TransactionServiceClient is the client API for TransactionService service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TransactionServiceClient interface {
	// NOTE: The reason why we have to create a separate rpc for each type
	// is because we cannot pass interfaces{}. Thus, we won't be able to specify
	// any value.
	// Ideally, we should call it only once
	// One of them should return an error stream.
	WriteIntTxn(ctx context.Context, in *IntTxn, opts ...grpc.CallOption) (*emptypb.Empty, error)
	WriteFloatTxn(ctx context.Context, in *FloatTxn, opts ...grpc.CallOption) (*emptypb.Empty, error)
	WriteStrTxn(ctx context.Context, in *StrTxn, opts ...grpc.CallOption) (*emptypb.Empty, error)
	WriteMapTxn(ctx context.Context, in *MapTxn, opts ...grpc.CallOption) (*emptypb.Empty, error)
	// This procedure has to be called only once to open a stream for errors.
	ProcessTxnErrors(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ProcessTxnErrorsClient, error)
	// Streaming rpcs: https://grpc.io/docs/languages/go/basics/
	// Open a stream for Int transactions.
	ReadIntTxns(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ReadIntTxnsClient, error)
	// Open a stream for Float transactions.
	ReadFloatTxns(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ReadFloatTxnsClient, error)
	// Open a stream for Str transactions.
	ReadStrTxn(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ReadStrTxnClient, error)
	// Open a stream for Map transactions.
	ReadMapTxn(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ReadMapTxnClient, error)
}

type transactionServiceClient struct {
	cc grpc.ClientConnInterface
}

func NewTransactionServiceClient(cc grpc.ClientConnInterface) TransactionServiceClient {
	return &transactionServiceClient{cc}
}

func (c *transactionServiceClient) WriteIntTxn(ctx context.Context, in *IntTxn, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, TransactionService_WriteIntTxn_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionServiceClient) WriteFloatTxn(ctx context.Context, in *FloatTxn, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, TransactionService_WriteFloatTxn_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionServiceClient) WriteStrTxn(ctx context.Context, in *StrTxn, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, TransactionService_WriteStrTxn_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionServiceClient) WriteMapTxn(ctx context.Context, in *MapTxn, opts ...grpc.CallOption) (*emptypb.Empty, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	out := new(emptypb.Empty)
	err := c.cc.Invoke(ctx, TransactionService_WriteMapTxn_FullMethodName, in, out, cOpts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *transactionServiceClient) ProcessTxnErrors(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ProcessTxnErrorsClient, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &TransactionService_ServiceDesc.Streams[0], TransactionService_ProcessTxnErrors_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &transactionServiceProcessTxnErrorsClient{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type TransactionService_ProcessTxnErrorsClient interface {
	Recv() (*Error, error)
	grpc.ClientStream
}

type transactionServiceProcessTxnErrorsClient struct {
	grpc.ClientStream
}

func (x *transactionServiceProcessTxnErrorsClient) Recv() (*Error, error) {
	m := new(Error)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *transactionServiceClient) ReadIntTxns(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ReadIntTxnsClient, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &TransactionService_ServiceDesc.Streams[1], TransactionService_ReadIntTxns_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &transactionServiceReadIntTxnsClient{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type TransactionService_ReadIntTxnsClient interface {
	Recv() (*IntTxn, error)
	grpc.ClientStream
}

type transactionServiceReadIntTxnsClient struct {
	grpc.ClientStream
}

func (x *transactionServiceReadIntTxnsClient) Recv() (*IntTxn, error) {
	m := new(IntTxn)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *transactionServiceClient) ReadFloatTxns(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ReadFloatTxnsClient, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &TransactionService_ServiceDesc.Streams[2], TransactionService_ReadFloatTxns_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &transactionServiceReadFloatTxnsClient{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type TransactionService_ReadFloatTxnsClient interface {
	Recv() (*FloatTxn, error)
	grpc.ClientStream
}

type transactionServiceReadFloatTxnsClient struct {
	grpc.ClientStream
}

func (x *transactionServiceReadFloatTxnsClient) Recv() (*FloatTxn, error) {
	m := new(FloatTxn)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *transactionServiceClient) ReadStrTxn(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ReadStrTxnClient, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &TransactionService_ServiceDesc.Streams[3], TransactionService_ReadStrTxn_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &transactionServiceReadStrTxnClient{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type TransactionService_ReadStrTxnClient interface {
	Recv() (*StrTxn, error)
	grpc.ClientStream
}

type transactionServiceReadStrTxnClient struct {
	grpc.ClientStream
}

func (x *transactionServiceReadStrTxnClient) Recv() (*StrTxn, error) {
	m := new(StrTxn)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

func (c *transactionServiceClient) ReadMapTxn(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (TransactionService_ReadMapTxnClient, error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &TransactionService_ServiceDesc.Streams[4], TransactionService_ReadMapTxn_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &transactionServiceReadMapTxnClient{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

type TransactionService_ReadMapTxnClient interface {
	Recv() (*MapTxn, error)
	grpc.ClientStream
}

type transactionServiceReadMapTxnClient struct {
	grpc.ClientStream
}

func (x *transactionServiceReadMapTxnClient) Recv() (*MapTxn, error) {
	m := new(MapTxn)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// TransactionServiceServer is the server API for TransactionService service.
// All implementations must embed UnimplementedTransactionServiceServer
// for forward compatibility
type TransactionServiceServer interface {
	// NOTE: The reason why we have to create a separate rpc for each type
	// is because we cannot pass interfaces{}. Thus, we won't be able to specify
	// any value.
	// Ideally, we should call it only once
	// One of them should return an error stream.
	WriteIntTxn(context.Context, *IntTxn) (*emptypb.Empty, error)
	WriteFloatTxn(context.Context, *FloatTxn) (*emptypb.Empty, error)
	WriteStrTxn(context.Context, *StrTxn) (*emptypb.Empty, error)
	WriteMapTxn(context.Context, *MapTxn) (*emptypb.Empty, error)
	// This procedure has to be called only once to open a stream for errors.
	ProcessTxnErrors(*emptypb.Empty, TransactionService_ProcessTxnErrorsServer) error
	// Streaming rpcs: https://grpc.io/docs/languages/go/basics/
	// Open a stream for Int transactions.
	ReadIntTxns(*emptypb.Empty, TransactionService_ReadIntTxnsServer) error
	// Open a stream for Float transactions.
	ReadFloatTxns(*emptypb.Empty, TransactionService_ReadFloatTxnsServer) error
	// Open a stream for Str transactions.
	ReadStrTxn(*emptypb.Empty, TransactionService_ReadStrTxnServer) error
	// Open a stream for Map transactions.
	ReadMapTxn(*emptypb.Empty, TransactionService_ReadMapTxnServer) error
	mustEmbedUnimplementedTransactionServiceServer()
}

// UnimplementedTransactionServiceServer must be embedded to have forward compatible implementations.
type UnimplementedTransactionServiceServer struct {
}

func (UnimplementedTransactionServiceServer) WriteIntTxn(context.Context, *IntTxn) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteIntTxn not implemented")
}
func (UnimplementedTransactionServiceServer) WriteFloatTxn(context.Context, *FloatTxn) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteFloatTxn not implemented")
}
func (UnimplementedTransactionServiceServer) WriteStrTxn(context.Context, *StrTxn) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteStrTxn not implemented")
}
func (UnimplementedTransactionServiceServer) WriteMapTxn(context.Context, *MapTxn) (*emptypb.Empty, error) {
	return nil, status.Errorf(codes.Unimplemented, "method WriteMapTxn not implemented")
}
func (UnimplementedTransactionServiceServer) ProcessTxnErrors(*emptypb.Empty, TransactionService_ProcessTxnErrorsServer) error {
	return status.Errorf(codes.Unimplemented, "method ProcessTxnErrors not implemented")
}
func (UnimplementedTransactionServiceServer) ReadIntTxns(*emptypb.Empty, TransactionService_ReadIntTxnsServer) error {
	return status.Errorf(codes.Unimplemented, "method ReadIntTxns not implemented")
}
func (UnimplementedTransactionServiceServer) ReadFloatTxns(*emptypb.Empty, TransactionService_ReadFloatTxnsServer) error {
	return status.Errorf(codes.Unimplemented, "method ReadFloatTxns not implemented")
}
func (UnimplementedTransactionServiceServer) ReadStrTxn(*emptypb.Empty, TransactionService_ReadStrTxnServer) error {
	return status.Errorf(codes.Unimplemented, "method ReadStrTxn not implemented")
}
func (UnimplementedTransactionServiceServer) ReadMapTxn(*emptypb.Empty, TransactionService_ReadMapTxnServer) error {
	return status.Errorf(codes.Unimplemented, "method ReadMapTxn not implemented")
}
func (UnimplementedTransactionServiceServer) mustEmbedUnimplementedTransactionServiceServer() {}

// UnsafeTransactionServiceServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TransactionServiceServer will
// result in compilation errors.
type UnsafeTransactionServiceServer interface {
	mustEmbedUnimplementedTransactionServiceServer()
}

func RegisterTransactionServiceServer(s grpc.ServiceRegistrar, srv TransactionServiceServer) {
	s.RegisterService(&TransactionService_ServiceDesc, srv)
}

func _TransactionService_WriteIntTxn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(IntTxn)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServiceServer).WriteIntTxn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TransactionService_WriteIntTxn_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServiceServer).WriteIntTxn(ctx, req.(*IntTxn))
	}
	return interceptor(ctx, in, info, handler)
}

func _TransactionService_WriteFloatTxn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(FloatTxn)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServiceServer).WriteFloatTxn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TransactionService_WriteFloatTxn_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServiceServer).WriteFloatTxn(ctx, req.(*FloatTxn))
	}
	return interceptor(ctx, in, info, handler)
}

func _TransactionService_WriteStrTxn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(StrTxn)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServiceServer).WriteStrTxn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TransactionService_WriteStrTxn_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServiceServer).WriteStrTxn(ctx, req.(*StrTxn))
	}
	return interceptor(ctx, in, info, handler)
}

func _TransactionService_WriteMapTxn_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(MapTxn)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TransactionServiceServer).WriteMapTxn(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: TransactionService_WriteMapTxn_FullMethodName,
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TransactionServiceServer).WriteMapTxn(ctx, req.(*MapTxn))
	}
	return interceptor(ctx, in, info, handler)
}

func _TransactionService_ProcessTxnErrors_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(TransactionServiceServer).ProcessTxnErrors(m, &transactionServiceProcessTxnErrorsServer{ServerStream: stream})
}

type TransactionService_ProcessTxnErrorsServer interface {
	Send(*Error) error
	grpc.ServerStream
}

type transactionServiceProcessTxnErrorsServer struct {
	grpc.ServerStream
}

func (x *transactionServiceProcessTxnErrorsServer) Send(m *Error) error {
	return x.ServerStream.SendMsg(m)
}

func _TransactionService_ReadIntTxns_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(TransactionServiceServer).ReadIntTxns(m, &transactionServiceReadIntTxnsServer{ServerStream: stream})
}

type TransactionService_ReadIntTxnsServer interface {
	Send(*IntTxn) error
	grpc.ServerStream
}

type transactionServiceReadIntTxnsServer struct {
	grpc.ServerStream
}

func (x *transactionServiceReadIntTxnsServer) Send(m *IntTxn) error {
	return x.ServerStream.SendMsg(m)
}

func _TransactionService_ReadFloatTxns_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(TransactionServiceServer).ReadFloatTxns(m, &transactionServiceReadFloatTxnsServer{ServerStream: stream})
}

type TransactionService_ReadFloatTxnsServer interface {
	Send(*FloatTxn) error
	grpc.ServerStream
}

type transactionServiceReadFloatTxnsServer struct {
	grpc.ServerStream
}

func (x *transactionServiceReadFloatTxnsServer) Send(m *FloatTxn) error {
	return x.ServerStream.SendMsg(m)
}

func _TransactionService_ReadStrTxn_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(TransactionServiceServer).ReadStrTxn(m, &transactionServiceReadStrTxnServer{ServerStream: stream})
}

type TransactionService_ReadStrTxnServer interface {
	Send(*StrTxn) error
	grpc.ServerStream
}

type transactionServiceReadStrTxnServer struct {
	grpc.ServerStream
}

func (x *transactionServiceReadStrTxnServer) Send(m *StrTxn) error {
	return x.ServerStream.SendMsg(m)
}

func _TransactionService_ReadMapTxn_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(emptypb.Empty)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(TransactionServiceServer).ReadMapTxn(m, &transactionServiceReadMapTxnServer{ServerStream: stream})
}

type TransactionService_ReadMapTxnServer interface {
	Send(*MapTxn) error
	grpc.ServerStream
}

type transactionServiceReadMapTxnServer struct {
	grpc.ServerStream
}

func (x *transactionServiceReadMapTxnServer) Send(m *MapTxn) error {
	return x.ServerStream.SendMsg(m)
}

// TransactionService_ServiceDesc is the grpc.ServiceDesc for TransactionService service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var TransactionService_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "transactions.TransactionService",
	HandlerType: (*TransactionServiceServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "WriteIntTxn",
			Handler:    _TransactionService_WriteIntTxn_Handler,
		},
		{
			MethodName: "WriteFloatTxn",
			Handler:    _TransactionService_WriteFloatTxn_Handler,
		},
		{
			MethodName: "WriteStrTxn",
			Handler:    _TransactionService_WriteStrTxn_Handler,
		},
		{
			MethodName: "WriteMapTxn",
			Handler:    _TransactionService_WriteMapTxn_Handler,
		},
	},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "ProcessTxnErrors",
			Handler:       _TransactionService_ProcessTxnErrors_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ReadIntTxns",
			Handler:       _TransactionService_ReadIntTxns_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ReadFloatTxns",
			Handler:       _TransactionService_ReadFloatTxns_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ReadStrTxn",
			Handler:       _TransactionService_ReadStrTxn_Handler,
			ServerStreams: true,
		},
		{
			StreamName:    "ReadMapTxn",
			Handler:       _TransactionService_ReadMapTxn_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "transactions/transactions.proto",
}
