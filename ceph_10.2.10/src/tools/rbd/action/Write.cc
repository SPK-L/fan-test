// -*- mode:C++; tab-width:8; c-basic-offset:2; indent-tabs-mode:t -*-
// vim: ts=8 sw=2 smarttab

#include "tools/rbd/ArgumentTypes.h"
#include "tools/rbd/Shell.h"
#include "tools/rbd/Utils.h"
#include "include/Context.h"
#include "common/blkdev.h"
#include "common/errno.h"
#include "common/Throttle.h"
#include "common/ceph_crypto.h"
#include <iostream>
#include <boost/program_options.hpp>
#include <boost/scoped_ptr.hpp>

namespace rbd {
namespace action {
namespace write {

namespace at = argument_types;
namespace po = boost::program_options;

using ceph::crypto::MD5;

static inline void buf_to_hex(const unsigned char *buf, int len, char *str)
{
  int i;
  str[0] = '\0';
  for (i = 0; i < len; i++) {
    sprintf(&str[i*2], "%02x", (int)buf[i]);
  }
}

class C_Write : public Context {
public:
  C_Write(SimpleThrottle &simple_throttle, librbd::Image &image,
           bufferlist &bl, uint64_t offset)
    : m_throttle(simple_throttle), m_image(image),
      m_aio_completion(
        new librbd::RBD::AioCompletion(this, &utils::aio_context_callback)),
      m_bufferlist(bl), m_offset(offset)
  {
  }

  void send()
  {
    m_throttle.start_op();

    int op_flags = LIBRADOS_OP_FLAG_FADVISE_SEQUENTIAL |
                   LIBRADOS_OP_FLAG_FADVISE_NOCACHE;
    int r = m_image.aio_write2(m_offset, m_bufferlist.length(), m_bufferlist,
                               m_aio_completion, op_flags);
    if (r < 0) {
      std::cerr << "rbd: error requesting write to destination image"
                << std::endl;
      m_aio_completion->release();
      m_throttle.end_op(r);
    }
  }

  virtual void finish(int r)
  {
    if (r < 0) {
      std::cerr << "rbd: error writing to destination image at offset "
                << m_offset << ": " << cpp_strerror(r) << std::endl;
    }
    m_throttle.end_op(r);
  }

private:
  SimpleThrottle &m_throttle;
  librbd::Image &m_image;
  librbd::RBD::AioCompletion *m_aio_completion;
  bufferlist m_bufferlist;
  uint64_t m_offset;
};

static int do_write(librbd::Image& image, uint64_t offset, uint64_t len,
                    const char *path, bool checksum, bool no_progress)
{
  int fd, r;
  librbd::image_info_t info;
  uint64_t img_size = 0;
  uint64_t filepos = 0;
  size_t blklen = 0;
  size_t reqlen = 0;
  ssize_t readlen = 0;
  struct stat stat_buf;
  uint64_t period = 0;
  char final_etag[CEPH_CRYPTO_MD5_DIGESTSIZE * 2];
  char final_etag_hex[CEPH_CRYPTO_MD5_DIGESTSIZE * 2 + 1];
  MD5 hash;

  utils::ProgressContext pc("Writing image", no_progress);

  r = image.stat(info, sizeof(info));
  if (r < 0) {
    r = -EFAULT ;
    cerr << "rbd: failed to get image stat" << std::endl;
    return r;
  }

  period = image.get_stripe_count() * (1ull << info.order);
  char *p = new char[period];
  if (p == NULL){
    cerr << "bad alloc for write"<< std::endl;
    return -ENOMEM;
  }

  boost::scoped_ptr<SimpleThrottle> throttle;
  bool from_stdin = !strcmp(path, "-");
  if (from_stdin) {
    throttle.reset(new SimpleThrottle(1, false));
    fd = 0;
  } else {
    uint64_t size = 0;
    throttle.reset(new SimpleThrottle(
          max(g_conf->rbd_concurrent_management_ops, 1), false));
    if ((fd = open(path, O_RDONLY)) < 0) {
      r = -errno;
      cerr << "rbd: error opening " << path << std::endl;
      goto done2;
    }
    if ((fstat(fd, &stat_buf)) < 0) {
      r = -errno;
      cerr << "rbd: stat error " << path << std::endl;
      goto done;
    }
    if (S_ISDIR(stat_buf.st_mode)) {
      r = -EISDIR;
      cerr << "rbd: cannot read a directory" << std::endl;
      goto done;
    }
    if (stat_buf.st_size)
      size = (uint64_t)stat_buf.st_size;

    if (!size) {
      int64_t bdev_size = 0;
      r = get_block_device_size(fd, &bdev_size);
      if (r < 0) {
        std::cerr << "rbd: unable to get size of file/block device"
                  << std::endl;
        goto done;
      }
      assert(bdev_size >= 0);
      size = (uint64_t) bdev_size;
    }
    if (len > size){
      r = -EFAULT;
      cerr << "rbd: file/(block device) size " << size << " < len " << len 
           << std::endl;
      goto done;

    }
    posix_fadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
  }
  //check between image size and offset/len
  r = image.size(&img_size);
  if (r < 0) {
    cerr << "rbd: failed to get image size" << std::endl;
    goto done;
  }
  if (offset > img_size) {
    r = -EFAULT;
    cerr << "rbd: offset overflow image size" << std::endl;
    goto done;
  }
  if ((offset + len) > img_size) {
    r = -EFAULT;
    cerr << "rbd: offset+len  overflow image size,please check" << std::endl;
    goto done;
  }
  // first compare checksum if not from stdin
  if (checksum && !from_stdin) {
    uint64_t chkret;
    reqlen = std::min(len, period);
    // loop body handles 0 return, as we may have a block to flush
    while ((readlen = ::read(fd, p + blklen, reqlen)) >= 0) {
      blklen += readlen;

      // if read was short, try again to fill the block before writing
      if (readlen && ((size_t)readlen < reqlen)) {
        reqlen -= readlen;
        continue;
      }

      filepos += blklen;
      if (readlen == 0)
        break;
      hash.Update((const byte *)p, blklen);
      blklen = 0;
      reqlen = std::min(len - filepos, period);
    }
    if (readlen < 0) {
      r = -errno;
      cerr << "rbd: read file failed:" << r << std::endl;
      goto done;
    }

    hash.Final((byte *)final_etag);
    buf_to_hex((const unsigned char *)final_etag, CEPH_CRYPTO_MD5_DIGESTSIZE,
               final_etag_hex);
    final_etag_hex[sizeof(final_etag_hex) - 1] = '\0';

    readlen = ::read(fd, final_etag, CEPH_CRYPTO_MD5_DIGESTSIZE * 2);
    if (readlen != CEPH_CRYPTO_MD5_DIGESTSIZE * 2) {
      r = -ENOENT;
      cerr << "read checksum from file failed: " << readlen << std::endl;
      goto done;
    }
    if (strcmp(final_etag, final_etag_hex)) {
      r = -EAGAIN;
      cerr << "rbd: file checksum is not same:" << std::endl;
      goto done;
    }
    chkret = lseek64(fd, 0, SEEK_SET);
    if (chkret != 0) {
      r = -errno;
      cerr << "rbd: error seeking input file to offset" << chkret << std::endl;
      goto done;
    }
    blklen = 0;
    filepos = 0;
  }

  //do write in loop, we read each time use max size 4M
  reqlen = std::min(len, period);
  // loop body handles 0 return, as we may have a block to flush
  while ((readlen = ::read(fd, p + blklen, reqlen)) >= 0) {
    if (throttle->pending_error()) {
      break;
    }

    blklen += readlen;
    // if read was short, try again to fill the block before writing
    if (readlen && ((size_t)readlen < reqlen)) {
      reqlen -= readlen;
      continue;
    }
    //update percentage
    if(!from_stdin)
      pc.update_progress(filepos, len);

    bufferlist bl(blklen);
    bl.append(p, blklen);
    if (from_stdin && (filepos + blklen > len)) {
      r = -EFAULT ;
      cerr << "rbd: write overflow "<< filepos + blklen << std::endl;
      goto done;
    }

    //skip writing zeros to create sparse images
    if (!bl.is_zero()) {
      C_Write *ctx = new C_Write(*throttle, image, bl, offset+filepos);
      ctx->send();
    }

    if (checksum && from_stdin) {
      hash.Update((const byte *)p, blklen);
    }
    filepos += blklen;
    if (readlen == 0)
      break;
    //each time to read need to init  blklen  and reqlen;
    blklen = 0;
    reqlen = std::min(len - filepos, period);
  }
  if (readlen < 0) {
    r = -errno;
    cerr << "rbd: read file failed:" << r << std::endl;
    goto done;
  }

  r = throttle->wait_for_ret();
  if (checksum && from_stdin) {
    hash.Final((byte *)final_etag);
    buf_to_hex((const unsigned char *)final_etag, CEPH_CRYPTO_MD5_DIGESTSIZE,
               final_etag_hex);
    final_etag_hex[sizeof(final_etag_hex) - 1] = '\0';
    readlen = ::read(fd, final_etag, CEPH_CRYPTO_MD5_DIGESTSIZE * 2);
    if (readlen != CEPH_CRYPTO_MD5_DIGESTSIZE * 2) {
      r = -ENOENT;
      cerr << "read checksum from file failed: " << readlen << std::endl;
      goto done;
    }
    if (strcmp(final_etag, final_etag_hex)) {
      r = -EAGAIN;
      cerr << "rbd: file checksum is not same:" << std::endl;
      goto done;
    }
  }

done:
  if (!from_stdin) {
    if (r < 0) 
      pc.fail();
    else
      pc.finish();
    close(fd);
  }
done2:
  delete[] p;

  return r;
}

void get_arguments(po::options_description *positional,
                   po::options_description *options) {
  at::add_image_spec_options(positional, options, at::ARGUMENT_MODIFIER_NONE);
  // make path-name at last
  at::add_path_options(positional, options, "read file (or '-' for stdin)");
  at::add_extent_options(options);
  at::add_checksum_option(options);
  at::add_no_progress_option(options);
}

int execute(const po::variables_map &vm) {
  size_t arg_index = 0;
  std::string pool_name;
  std::string image_name;
  std::string snap_name;
  int r = utils::get_pool_image_snapshot_names(
    vm, at::ARGUMENT_MODIFIER_NONE, &arg_index, &pool_name, &image_name,
    &snap_name, utils::SNAPSHOT_PRESENCE_NONE, utils::SPEC_VALIDATION_NONE);
  if (r < 0) {
    return r;
  }

  std::string path;
  r = utils::get_path(vm, utils::get_positional_argument(vm, 1), &path);
  if (r < 0) {
    return r;
  }

  uint64_t offset, len;
  bool checksum = vm["checksum"].as<bool>();
  r = utils::get_extent_options(vm, &offset, &len);
  if (r < 0) {
    return r;
  }

  librados::Rados rados;
  librados::IoCtx io_ctx;
  librbd::Image image;
  r = utils::init_and_open_image(pool_name, image_name, "", false,
                                 &rados, &io_ctx, &image);
  if (r < 0) {
    return r;
  }

  r = do_write(image, offset, len, path.c_str(), checksum,
               vm[at::NO_PROGRESS].as<bool>());
  if (r < 0) {
    std::cerr << "rbd: write error: " << cpp_strerror(r) << std::endl;
    return r;
  }
  return 0;
}

Shell::SwitchArguments switched_arguments({"checksum"});
Shell::Action action(
  {"write"}, {}, "write special place of the image.", "",
  &get_arguments, &execute);

} // namespace write
} // namespace action
} // namespace rbd
