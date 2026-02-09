import unittest
from main import FileManager

class TestFileManager(unittest.TestCase):

    # Valid mappings
    def test_map_music_file(self):
        fm = FileManager("config.json")
        self.assertEqual(fm.map_local_to_remote("/home/user/file.wav"), "files/Music/file.wav")

    def test_map_picture_file(self):
        fm = FileManager("config.json")
        self.assertEqual(fm.map_local_to_remote("/home/user/photo.jpg"), "files/Pictures/photo.jpg")

    def test_map_document_file(self):
        fm = FileManager("config.json")
        self.assertEqual(fm.map_local_to_remote("/home/user/report.pdf"), "files/Documents/report.pdf")

    def test_map_nested_path(self):
        fm = FileManager("config.json")
        self.assertEqual(fm.map_local_to_remote("/home/user/Music/album/song.mp3"), "files/Music/album/song.mp3")

    def test_map_file_with_uppercase_extension(self):
        fm = FileManager("config.json")
        self.assertEqual(fm.map_local_to_remote("/home/user/PICTURE.PNG"), "files/Pictures/PICTURE.PNG")

    # Invalid mappings
    def test_map_unknown_extension(self):
        fm = FileManager("config.json")
        with self.assertRaises(ValueError):
            fm.map_local_to_remote("/home/user/archive.zip")

    def test_map_file_with_no_extension(self):
        fm = FileManager("config.json")
        with self.assertRaises(ValueError):
            fm.map_local_to_remote("/home/user/README")

    def test_map_file_with_weird_extension(self):
        fm = FileManager("config.json")
        with self.assertRaises(ValueError):
            fm.map_local_to_remote("/home/user/file.abc")

if __name__ == "__main__":
    unittest.main()
