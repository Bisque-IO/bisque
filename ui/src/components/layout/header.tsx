import { useAuthStore } from "@/stores/auth"
import { useNavigate } from "react-router"
import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { Badge } from "@/components/ui/badge"
import { LogOut, User, Menu } from "lucide-react"
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet"
import { MobileSidebar } from "./mobile-sidebar"
import { ClusterSwitcher } from "../cluster-switcher"

interface HeaderProps {
  wsConnected: boolean
}

export function Header({ wsConnected }: HeaderProps) {
  const { username, accountName, tenantName, tenantId, logout } = useAuthStore()
  const navigate = useNavigate()

  const handleLogout = () => {
    logout()
    navigate("/login")
  }

  return (
    <header className="flex h-14 items-center border-b px-4 gap-4">
      <Sheet>
        <SheetTrigger asChild>
          <Button variant="ghost" size="icon" className="md:hidden">
            <Menu className="h-5 w-5" />
          </Button>
        </SheetTrigger>
        <SheetContent side="left" className="w-60 p-0">
          <MobileSidebar />
        </SheetContent>
      </Sheet>

      <ClusterSwitcher />

      <div className="flex-1" />

      <Badge variant={wsConnected ? "default" : "secondary"} className="text-xs">
        {wsConnected ? "Connected" : "Disconnected"}
      </Badge>

      {accountName && (
        <span className="text-sm text-muted-foreground">
          {accountName}
        </span>
      )}

      {tenantName && (
        <span className="text-sm text-muted-foreground">
          {tenantName} (#{tenantId})
        </span>
      )}

      <DropdownMenu>
        <DropdownMenuTrigger asChild>
          <Button variant="ghost" size="icon">
            <User className="h-4 w-4" />
          </Button>
        </DropdownMenuTrigger>
        <DropdownMenuContent align="end">
          {username && (
            <>
              <DropdownMenuItem disabled className="text-xs text-muted-foreground">
                {username}
              </DropdownMenuItem>
              <DropdownMenuSeparator />
            </>
          )}
          <DropdownMenuItem onClick={handleLogout}>
            <LogOut className="h-4 w-4 mr-2" />
            Logout
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>
    </header>
  )
}
