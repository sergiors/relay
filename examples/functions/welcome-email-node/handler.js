export function handler(event) {
  const user = event.new_image

  console.log(`Sending welcome email to ${user.email}`)
}
