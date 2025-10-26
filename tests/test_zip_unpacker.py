import os
import zipfile

from zip_unpacker import unpack_zip


def test_unpack_zip_with_csv_and_others(tmp_path):
    zip_path = tmp_path / 'sample.zip'
    out_dir = tmp_path / 'out'

    with zipfile.ZipFile(zip_path, 'w') as zf:
        zf.writestr('data/file1.csv', 'a,b\n1,2\n')
        zf.writestr('readme.txt', 'hello')

    extracted = unpack_zip(str(zip_path), str(out_dir))
    assert len(extracted) == 1
    assert os.path.normpath(extracted[0]).endswith(os.path.join('data', 'file1.csv'))
    assert os.path.exists(extracted[0])


def test_unpack_invalid_zip(tmp_path):
    zip_path = tmp_path / 'not_a_zip.zip'
    out_dir = tmp_path / 'out'
    zip_path.write_bytes(b'not a real zip')

    extracted = unpack_zip(str(zip_path), str(out_dir))
    assert extracted == []
