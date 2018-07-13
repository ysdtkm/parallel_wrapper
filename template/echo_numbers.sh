set -e
x=1
y=2
echo "x is ${x}, y is ${y}" > out

if [ ${x} = "3" ]; then
  exit 1
fi
