include "shared.thrift"

namespace cpp metadataServer
namespace py metadataServer
namespace java metadataServer

/* we can use shared.<datatype>, instead we could also typedef them for
	convenience */
typedef shared.response response
typedef shared.file file
typedef shared.uploadResponse uploadResponse

service MetadataServerService {

	void gossip(),
	file updateFile(1: file f),
	file getFile(1: string filename, 2: i32 v),
	uploadResponse storeFile(1: file f),
	void deleteFromServer(1: string filename, 2: i32 q),
	response deleteFile(1: string filename)
}
