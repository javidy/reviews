if [ -f $input_filename ]; then
   echo 'Archiving file '$input_filename
   mv $input_filename $archive_dir
fi