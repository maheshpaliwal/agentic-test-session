// Package tmongo records MongoDB commands into the active test session using
// the official driver's (v2) command monitoring.
//
//	opts := options.Client().ApplyURI(uri)
//	tmongo.Apply(opts)                  // one-liner; chains any existing monitor
//	client, _ := mongo.Connect(opts)
//
// Session attribution. In driver v1 a CommandMonitor receives no context, which
// would force a wrapper type around every Collection method. Driver v2 passes
// the per-operation context.Context to Started/Succeeded/Failed (the same ctx
// given to InsertOne, Find, ...), so the monitor can call core.Begin directly
// and no Collection wrapper is needed. Started and Succeeded/Failed are
// correlated by RequestID. Commands issued without a session ctx (heartbeats,
// background work) produce no events.
//
// The in-flight table is bounded: a command still unanswered after 10 minutes
// is recorded with the flag "unfinished" once the table (4096 entries) fills
// up, and a command started while it is full is recorded the same way.
//
// Writes yield core.Mutation values per applied statement
// (insert/update/delete): statements listed in writeErrors are left out, and
// so are the ones after the first failure of an ordered command, which the
// server never runs. findAndModify exposes the document the server returns as the Before image
// (new:false or remove) or the After image (new:true). A monitor cannot alter
// the command, so the opposite image is not available.
package tmongo
